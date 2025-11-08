use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
};

use log::{error, info, trace, warn};
use prost::Message as _;
use tokio::sync::{oneshot, Mutex};

use crate::{
    config::AtomicConfig,
    crypto::CachedBlock,
    proto::{
        checkpoint::{proto_backfill_nack::Origin, ProtoBackfillNack, ProtoBlockHint},
        consensus::{HalfSerializedBlock, ProtoAppendEntries, ProtoFork, ProtoViewChange},
        rpc::{proto_payload::Message, ProtoPayload},
    },
    rpc::{client::PinnedClient, MessageRef, PinnedMessage},
    utils::{
        channel::{Receiver, Sender},
        get_parent_hash_in_proto_block_ser, StorageServiceConnector,
    },
};

/// Deletes older blocks in favor of newer ones.
/// If the cache is full, and the block being put() has a lower n than the oldest block in the cache,
/// it is a Noop.
/// Since reading GC blocks always forms the pattern of (read parent hash) -> (fetch block) -> (read parent hash) -> ...
/// There is no need to adjust the position of the block in the cache.
struct ReadCache {
    cache: BTreeMap<u64, CachedBlock>,
    working_set_size: usize,
}

impl ReadCache {
    pub fn new(working_set_size: usize) -> Self {
        if working_set_size == 0 {
            panic!("Working set size cannot be 0");
        }
        ReadCache {
            cache: BTreeMap::new(),
            working_set_size,
        }
    }

    /// Return vals:
    /// - Ok(block) if the block is in the cache.
    /// - Err(block) block with the least n higher than the requested block, if the block is not in the cache.
    /// - Err(None) if the cache is just empty.
    pub fn get(&mut self, n: u64) -> Result<CachedBlock, Option<CachedBlock>> {
        if self.cache.is_empty() {
            return Err(None);
        }

        let block = self.cache.get(&n).cloned();
        if let Some(block) = block {
            return Ok(block);
        }

        let next_block = match self.cache.range(n..).next() {
            Some((_, block)) => block.clone(),
            None => {
                return Err(None);
            }
        };
        Err(Some(next_block))
    }

    pub fn put(&mut self, block: CachedBlock) {
        if self.cache.len() >= self.working_set_size
            && block.block.n < *self.cache.first_entry().unwrap().key()
        {
            // Don't put this in the cache.
            return;
        }
        if self.cache.len() >= self.working_set_size {
            self.cache.first_entry().unwrap().remove();
        }

        self.cache.insert(block.block.n, block);
    }
}

pub enum LogServerQuery {
    CheckHash(
        u64,     /* block.n */
        Vec<u8>, /* block_hash */
        Sender<bool>,
    ),
    GetHints(
        u64, /* last needed block.n */
        Sender<Vec<ProtoBlockHint>>,
    ),
}

pub enum LogServerCommand {
    NewBlock(CachedBlock, String /* sender_name */),
    Rollback(u64),
    UpdateBCI(u64),
}

pub struct LogServer {
    config: AtomicConfig,
    client: PinnedClient,
    bci: u64,

    logserver_rx: Receiver<LogServerCommand>,
    backfill_request_rx: Receiver<ProtoBackfillNack>,
    gc_rx: Receiver<u64>,

    query_rx: Receiver<LogServerQuery>,

    storage: StorageServiceConnector,
    /// Each node maintains n hash chains (lanes), one for each node in the system.
    /// The index corresponds to the node's position in the node_list.
    lanes: Vec<VecDeque<CachedBlock>>,

    /// LFU read cache for GCed blocks.
    read_cache: ReadCache,
}

const LOGSERVER_READ_CACHE_WSS: usize = 100;

impl LogServer {
    pub fn new(
        config: AtomicConfig,
        client: PinnedClient,
        logserver_rx: Receiver<LogServerCommand>,
        backfill_request_rx: Receiver<ProtoBackfillNack>,
        gc_rx: Receiver<u64>,
        query_rx: Receiver<LogServerQuery>,
        storage: StorageServiceConnector,
    ) -> Self {
        // Initialize n lanes, one for each node in the system
        let num_nodes = config.get().consensus_config.node_list.len();
        let lanes = (0..num_nodes).map(|_| VecDeque::new()).collect::<Vec<_>>();

        LogServer {
            config,
            client,
            logserver_rx,
            backfill_request_rx,
            gc_rx,
            query_rx,
            storage,
            lanes,
            read_cache: ReadCache::new(LOGSERVER_READ_CACHE_WSS),
            bci: 0,
        }
    }

    pub async fn run(logserver: Arc<Mutex<Self>>) {
        let mut logserver = logserver.lock().await;
        loop {
            if let Err(_) = logserver.worker().await {
                break;
            }
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            biased;
            cmd = self.logserver_rx.recv() => {
                match cmd {
                    Some(LogServerCommand::NewBlock(block, sender_name)) => {
                        trace!("Received block {} from {}", block.block.n, sender_name);
                        self.handle_new_block(block, sender_name).await;
                    },
                    Some(LogServerCommand::Rollback(n)) => {
                        trace!("Rolling back to block {}", n);
                        self.handle_rollback(n).await;
                    },
                    Some(LogServerCommand::UpdateBCI(n)) => {
                        trace!("Updating BCI to {}", n);
                        self.bci = n;
                    },
                    None => {
                        error!("LogServerCommand channel closed");
                        return Err(());
                    }
                }
            },

            gc_req = self.gc_rx.recv() => {
                if let Some(gc_req) = gc_req {
                    // GC all lanes up to gc_req
                    for lane in self.lanes.iter_mut() {
                        lane.retain(|block| block.block.n > gc_req);
                    }
                }
            },

            backfill_req = self.backfill_request_rx.recv() => {
                if let Some(backfill_req) = backfill_req {
                    self.respond_backfill(backfill_req).await?;
                }
            },

            query = self.query_rx.recv() => {
                if let Some(query) = query {
                    self.handle_query(query).await;
                }
            }
        }

        Ok(())
    }

    /// Get the lane index for a given sender name.
    /// Returns None if the sender is not in the node list.
    // HACK: This is gross, fix it later
    fn get_lane_index(&self, sender_name: &str) -> Option<usize> {
        let config = self.config.get();
        config
            .consensus_config
            .node_list
            .iter()
            .position(|name| name == sender_name)
    }

    /// Get a block by sequence number from a specific lane.
    /// TODO: For dissemination, we may want to search specific lanes or all lanes depending on context.
    /// For now, this searches the first lane (typically the node's own lane or leader's lane).
    async fn get_block(&mut self, n: u64) -> Option<CachedBlock> {
        self.get_block_from_lane(n, 0).await
    }

    /// Get a block by sequence number from a specific lane.
    async fn get_block_from_lane(&mut self, n: u64, lane_idx: usize) -> Option<CachedBlock> {
        if lane_idx >= self.lanes.len() {
            return None;
        }

        let lane = &self.lanes[lane_idx];
        let last_n = lane.back()?.block.n;
        if n == 0 || n > last_n {
            return None;
        }

        let first_n = lane.front()?.block.n;
        if n < first_n {
            return self.get_gced_block_from_lane(n, lane_idx).await;
        }

        let block_idx = lane.binary_search_by(|e| e.block.n.cmp(&n)).ok()?;
        let block = lane[block_idx].clone();

        Some(block)
    }

    /// Get a GCed block from a specific lane using the read cache and storage.
    async fn get_gced_block_from_lane(&mut self, n: u64, lane_idx: usize) -> Option<CachedBlock> {
        if lane_idx >= self.lanes.len() {
            return None;
        }

        let lane = &self.lanes[lane_idx];
        let first_n = lane.front()?.block.n;
        if n >= first_n {
            return None; // The block is not GCed.
        }

        // Search in the read cache.
        let starting_point = match self.read_cache.get(n) {
            Ok(block) => {
                return Some(block);
            }
            Err(Some(block)) => block,
            Err(None) => {
                // Get the first block in the lane.
                lane.front()?.clone()
            }
        };

        // Fetch the block from the storage.
        let mut ret = starting_point;
        while ret.block.n > n {
            let parent_hash = &ret.block.parent;
            let block = self
                .storage
                .get_block(parent_hash)
                .await
                .expect("Failed to get block from storage");
            self.read_cache.put(block.clone());
            ret = block;
        }

        Some(ret)
    }

    async fn respond_backfill(&mut self, backfill_req: ProtoBackfillNack) -> Result<(), ()> {
        let sender = backfill_req.reply_name;
        let hints = backfill_req.hints;
        let existing_fork = match &backfill_req.origin {
            Some(Origin::Ae(ae)) => match ae.fork.as_ref() {
                Some(fork) => fork,
                None => {
                    warn!("Malformed request");
                    return Ok(());
                }
            },

            Some(Origin::Vc(vc)) => match vc.fork.as_ref() {
                Some(fork) => fork,
                None => {
                    warn!("Malformed request");
                    return Ok(());
                }
            },

            None => {
                warn!("Malformed request");
                return Ok(());
            }
        };

        let last_n = match existing_fork.serialized_blocks.last() {
            Some(block) => block.n,
            None => {
                // TODO: For dissemination, determine which lane to use for backfill
                // For now, use the first lane (leader's lane)
                match self.lanes.get(0).and_then(|lane| lane.back()) {
                    Some(block) => block.block.n,
                    None => 0,
                }
            }
        };

        let first_n = backfill_req.last_index_needed;

        let new_fork = self.fill_fork(first_n, last_n, hints).await;

        let payload = match backfill_req.origin.unwrap() {
            Origin::Ae(ae) => ProtoPayload {
                message: Some(Message::AppendEntries(ProtoAppendEntries {
                    fork: Some(new_fork),
                    is_backfill_response: true,
                    ..ae
                })),
            },

            Origin::Vc(vc) => ProtoPayload {
                message: Some(Message::ViewChange(ProtoViewChange {
                    fork: Some(new_fork),
                    ..vc
                })),
            },
        };

        // Send the payload to the sender.
        let buf = payload.encode_to_vec();

        let _ = PinnedClient::send(
            &self.client,
            &sender,
            MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon),
        )
        .await;

        Ok(())
    }

    /// Returns a fork that contains blocks from `first_n` to `last_n` (both inclusive).
    /// During the process, if one of my blocks matches in hints, we stop.
    async fn fill_fork(
        &mut self,
        first_n: u64,
        last_n: u64,
        mut hints: Vec<ProtoBlockHint>,
    ) -> ProtoFork {
        if last_n < first_n {
            panic!("Invalid range");
        }

        let hint_map = hints
            .drain(..)
            .map(|hint| (hint.block_n, hint.digest))
            .collect::<HashMap<_, _>>();

        let mut fork_queue = VecDeque::with_capacity((last_n - first_n + 1) as usize);

        for i in (first_n..=last_n).rev() {
            let block = match self.get_block(i).await {
                Some(block) => block,
                None => {
                    warn!("Block {} not found", i);
                    break;
                }
            };

            let hint = hint_map.get(&i);
            if let Some(hint) = hint {
                if hint.eq(&block.block_hash) {
                    break;
                }
            }

            fork_queue.push_front(block);
        }

        ProtoFork {
            serialized_blocks: fork_queue
                .into_iter()
                .map(|block| HalfSerializedBlock {
                    n: block.block.n,
                    view: block.block.view,
                    view_is_stable: block.block.view_is_stable,
                    config_num: block.block.config_num,
                    serialized_body: block.block_ser.clone(),
                })
                .collect(),
        }
    }

    async fn handle_query(&mut self, query: LogServerQuery) {
        match query {
            LogServerQuery::CheckHash(n, hsh, sender) => {
                if n == 0 {
                    sender.send(true).await.unwrap();
                    return;
                }

                let block = match self.get_block(n).await {
                    Some(block) => block,
                    None => {
                        // TODO: For dissemination, determine which lane to query
                        // For now, report last_n from the first lane (leader's lane)
                        let last_n_seen = self
                            .lanes
                            .get(0)
                            .and_then(|lane| lane.back())
                            .map_or(0, |block| block.block.n);
                        error!(
                            "Block {} not found, last_n seen in lane 0: {}",
                            n, last_n_seen
                        );
                        sender.send(false).await.unwrap();
                        return;
                    }
                };

                sender.send(block.block_hash.eq(&hsh)).await.unwrap();
            }
            LogServerQuery::GetHints(last_needed_n, sender) => {
                // Starting from last_needed_n,
                // Include last_needed_n, last_needed_n + 1000, last_needed_n + 2000, ..., until last_needed_n + 10000,
                // Then include last_needed_n + 10000, last_needed_n + 20000, ..., until last_needed_n + 100000,
                // and so on until we reach last_n. Also include the last_n.

                const JUMP_START: u64 = 1000;
                const JUMP_MULTIPLIER: u64 = 10;

                let mut hints = Vec::new();

                // TODO: For dissemination, determine which lane to use for hints
                // For now, use the first lane (leader's lane)
                let last_n = self
                    .lanes
                    .get(0)
                    .and_then(|lane| lane.back())
                    .map_or(0, |block| block.block.n);
                let mut curr_n = last_needed_n;
                let mut curr_jump = JUMP_START;
                let mut curr_jump_used_for = 0;

                if curr_n == 0 {
                    curr_n = 1;
                }

                while curr_n < last_n {
                    let block = match self.get_block(curr_n).await {
                        Some(block) => block,
                        None => {
                            break;
                        }
                    };
                    hints.push(ProtoBlockHint {
                        block_n: block.block.n,
                        digest: block.block_hash.clone(),
                    });

                    curr_n += curr_jump;
                    curr_jump_used_for += 1;
                    if curr_jump_used_for >= JUMP_MULTIPLIER {
                        curr_jump *= JUMP_MULTIPLIER;
                        curr_jump_used_for = 0;
                    }
                }

                // Also add last_n.
                if last_n > 0 {
                    let block = match self.get_block(last_n).await {
                        Some(block) => block,
                        None => {
                            // This should never happen.
                            panic!("Block {} not found", last_n);
                        }
                    };
                    hints.push(ProtoBlockHint {
                        block_n: block.block.n,
                        digest: block.block_hash.clone(),
                    });
                }

                let len = hints.len();

                let res = sender.send(hints).await;
                info!("Sent hints size {}, result = {:?}", len, res);
            }
        }
    }

    /// Invariant: Each lane (hash chain) is continuous with increasing seq num and maintains hash chain continuity
    async fn handle_new_block(&mut self, block: CachedBlock, sender_name: String) {
        let lane_idx = match self.get_lane_index(&sender_name) {
            Some(idx) => idx,
            None => {
                error!("Sender {} not in node list", sender_name);
                return;
            }
        };

        let lane = &mut self.lanes[lane_idx];
        let last_n = lane.back().map_or(0, |block| block.block.n);
        if block.block.n != last_n + 1 {
            error!(
                "Block {} from {} is not the next block, last_n: {}",
                block.block.n, sender_name, last_n
            );
            return;
        }

        if last_n > 0 && !block.block.parent.eq(&lane.back().unwrap().block_hash) {
            error!(
                "Parent hash mismatch for block {} from {}",
                block.block.n, sender_name
            );
            return;
        }

        lane.push_back(block);
    }

    async fn handle_rollback(&mut self, mut n: u64) {
        if n <= self.bci {
            n = self.bci + 1;
        }

        // Rollback all lanes to block n or earlier
        for lane in self.lanes.iter_mut() {
            lane.retain(|block| block.block.n <= n);
        }

        // Clean up read cache.
        self.read_cache.cache.retain(|k, _| *k <= n);
    }
}
