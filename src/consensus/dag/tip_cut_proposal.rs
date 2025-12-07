use std::collections::HashMap;
use std::time::Duration;
/// Tip Cut Proposal Module for DAG Consensus
///
/// This module is responsible for proposing tip cuts in the DAG.
/// Only the leader proposes tip cuts for each view.
///
/// Flow:
/// 1. Leader periodically checks if a new tip cut should be proposed
/// 2. Query lane_staging for the current tip cut (one CAR per lane)
/// 3. Construct ProtoTipCut message with the tip cut information
/// 4. Send ProtoTipCut to BlockSequencer for sequencing and broadcasting
/// 5. Nodes vote on the tip cut via tip_cut_voting component
///
/// Leadership:
/// - Only the current leader proposes tip cuts
/// - View changes update leadership via command channel
/// - Non-leaders skip proposal logic
use std::{cmp, sync::Arc};

use log::{debug, error, info, trace, warn};
use tokio::sync::{oneshot, Mutex};

use crate::{
    config::AtomicConfig,
    proto::consensus::ProtoBlockCar,
    utils::{
        channel::{Receiver, Sender},
        timer::ResettableTimer,
    },
};

use super::lane_staging::{LaneStagingQuery, TipCut};

pub type RawTipCut = Vec<ProtoBlockCar>;

/// Commands to control TipCutProposal behavior
#[derive(Debug, Clone)]
pub enum TipCutProposalCommand {
    NewUnstableView(u64 /* view num */, u64 /* config num */), // View changed to a new view, it is not stable, so don't propose new blocks.
    ViewStabilised(u64 /* view num */, u64 /* config num */), // View is stable now, if I am the leader in this view, propose new blocks.
    NewViewMessage(
        u64, /* view num */
        u64, /* config num */
             // HashType, /* new parent hash */
             // u64,      /* new seq num */
    ), // Change view to unstable, use ProtoForkValidation to propose a new view message.
}

/// TipCutProposal is responsible for periodically proposing tip cuts.
/// Only the leader proposes tip cuts.
pub struct TipCutProposal {
    config: AtomicConfig,

    // Current state
    ci: u64,
    view: u64,
    view_is_stable: bool,
    config_num: u64,

    // Leadership state
    i_am_leader: bool,
    // current_leader: String, // unused; leader derived from config per view

    // Timer for periodic proposals
    tip_cut_timer: Arc<std::pin::Pin<Box<ResettableTimer>>>,
    // Max Cars - if we have this many CARs, propose tip cut immediately (override timer)
    // If max_cars is 0, this feature is disabled
    tip_cut_max_cars: usize,

    // Query channel to LaneStaging
    lane_staging_query_tx: Sender<LaneStagingQuery>,

    // Send tip cuts to BlockSequencer for wrapping and broadcasting
    consensus_sequencer_tx: Sender<RawTipCut>,

    // Command channel for view changes and leadership updates
    cmd_rx: Receiver<TipCutProposalCommand>,

    // Per-lane watermark of last proposed CAR sequence number.
    // Ensures a given CAR for a lane is only included in one tip cut on this node.
    last_proposed_per_lane: HashMap<String, u64>,
}

impl TipCutProposal {
    pub fn new(
        config: AtomicConfig,
        lane_staging_query_tx: Sender<LaneStagingQuery>,
        consensus_sequencer_tx: Sender<RawTipCut>,
        cmd_rx: Receiver<TipCutProposalCommand>,
    ) -> Self {
        // Get initial configuration
        let config_snapshot = config.get();

        // Set up timer for periodic tip cut proposals
        let tip_cut_delay_ms = config_snapshot.dag_config.tip_cut_delay_ms;
        let tip_cut_timer = ResettableTimer::new(Duration::from_millis(tip_cut_delay_ms));
        let tip_cut_max_cars = config_snapshot.dag_config.tip_cut_max_cars;

        // Determine initial leadership
        #[cfg(feature = "view_change")]
        let (view, i_am_leader, current_leader, view_is_stable, config_num) = {
            let my_name = &config_snapshot.net_config.name;
            let leader = config_snapshot.consensus_config.get_leader_for_view(0);
            (0, leader == *my_name, leader, false, 0)
        };

        #[cfg(not(feature = "view_change"))]
        let (view, i_am_leader, current_leader, view_is_stable, config_num) = {
            let my_name = &config_snapshot.net_config.name;
            let leader = config_snapshot.consensus_config.get_leader_for_view(1);
            (1, leader == *my_name, leader, true, 1)
        };

        debug!(
            "TipCutProposal initialized: view={}, i_am_leader={}, leader={}, view_is_stable={}, config_num={}",
            view, i_am_leader, current_leader, view_is_stable, config_num
        );

        Self {
            config,
            ci: 0,
            view,
            view_is_stable,
            config_num,
            i_am_leader,
            // current_leader,
            tip_cut_timer,
            tip_cut_max_cars,
            lane_staging_query_tx,
            consensus_sequencer_tx,
            cmd_rx,
            last_proposed_per_lane: HashMap::new(),
        }
    }

    pub async fn run(tip_cut_proposal: Arc<Mutex<Self>>) {
        let mut tip_cut_proposal = tip_cut_proposal.lock().await;

        // Start the timer
        let timer_handle = tip_cut_proposal.tip_cut_timer.run().await;

        info!("TipCutProposal worker starting");

        loop {
            if let Err(_) = tip_cut_proposal.worker().await {
                break;
            }
        }

        timer_handle.abort();
        info!("TipCutProposal worker stopped");
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let mut timer_tick = false;
        let mut cmd = None;

        tokio::select! {
            biased;
            _cmd = self.cmd_rx.recv() => {
                cmd = _cmd;
            },
            _tick = self.tip_cut_timer.wait() => {
                timer_tick = _tick;
            }
        }

        // Handle commands (leadership updates, view changes)
        if let Some(command) = cmd {
            self.handle_command(command);
            return Ok(());
        }

        // If timer has ticked or enough cars have been seen to propose tip cut
        // Check if I am the leader and propose a tip cut
        if timer_tick {
            // If timer ticked, propose tip cut based on timer
            if self.i_am_leader() {
                if let Err(_) = self.propose_tip_cut(false).await {
                    error!("Failed to propose tip cut");
                }
            } else {
                trace!(
                    "Skipping tip cut proposal: i_am_leader={}",
                    self.i_am_leader
                );
            }
            return Ok(());
        } else if self.config.get().dag_config.tip_cut_max_cars > 0 {
            // Otherwise, check if enough CARs have been seen to propose tip cut
            if self.i_am_leader() {
                if let Err(_) = self.propose_tip_cut(true).await {
                    error!("Failed to propose tip cut");
                }
            } else {
                trace!(
                    "Skipping tip cut proposal: i_am_leader={}",
                    self.i_am_leader
                );
            }
            return Ok(());
        }

        // Channel closed
        Err(())
    }

    fn i_am_leader(&self) -> bool {
        let config = self.config.get();
        let leader = config.consensus_config.get_leader_for_view(self.view);
        leader == config.net_config.name
    }

    fn handle_command(&mut self, cmd: TipCutProposalCommand) {
        debug!("TipCutProposal received command: {:?}", cmd);
        match cmd {
            // Follow the changes, no questions asked!
            TipCutProposalCommand::NewUnstableView(v, c) => {
                self.view = v;
                self.config_num = c;
                self.view_is_stable = false;
            }
            TipCutProposalCommand::ViewStabilised(v, c) => {
                self.view = v;
                self.config_num = c;
                self.view_is_stable = true;
            }
            TipCutProposalCommand::NewViewMessage(v, c) => {
                warn!("Request for new view message: view: {} config: {}", v, c);
                self.view = v;
                self.config_num = c;
                self.view_is_stable = false;
            }
        }
    }

    /// Query lane_staging for current tip cut and broadcast it to all nodes.
    async fn propose_tip_cut(&mut self, use_threshold: bool) -> Result<(), ()> {
        debug!(
            "Proposing tip cut for view {} (ci={}), use_threshold={}",
            self.view, self.ci, use_threshold
        );
        // Query LaneStaging for the current tip cut
        let tip_cut = match self.query_tip_cut().await? {
            Some(tc) => tc,
            None => {
                debug!("No CARs available yet for tip cut proposal");
                // On timer ticks, still send an empty tip cut as heartbeat; on threshold path, skip.
                if use_threshold {
                    return Ok(());
                }
                self.send_tip_cut(vec![]).await?;
                // Do not update watermark (no cars)
                // Reset timer to mimic batch proposer heartbeat behavior
                self.tip_cut_timer.reset();
                return Ok(());
            }
        };

        // Filter CARs per lane using watermark (only propose cars with n > last_proposed)
        let mut filtered: Vec<ProtoBlockCar> = Vec::new();
        for (lane_id, car) in tip_cut.cars.into_iter() {
            let last = self
                .last_proposed_per_lane
                .get(&lane_id)
                .copied()
                .unwrap_or(0);
            if car.n > last {
                filtered.push(car);
            } else {
                trace!(
                    "Skipping already proposed CAR: lane={} car_n={} last_proposed_n={}",
                    lane_id,
                    last,
                    last
                );
            }
        }

        // Threshold path: only propose if enough new cars
        // The timer has not ticked, so we only propose if we have enough new cars
        if use_threshold {
            let have = filtered.len();
            let need = self.tip_cut_max_cars;
            if need > 0 && have < need {
                warn!(
                    "Not enough NEW CARs for threshold tip cut: have {} need {} (after watermark)",
                    have, need
                );
                return Ok(());
            }
        }

        // Timer path: allow empty heartbeat tip cuts
        if !use_threshold && filtered.is_empty() {
            debug!("Timer tick with no new CARs — sending empty tip cut heartbeat");
            self.send_tip_cut(vec![]).await?;
            self.tip_cut_timer.reset();
            return Ok(());
        }

        debug!(
            "Proposing tip cut with {} NEW CARs for view {} (ci={})",
            filtered.len(),
            self.view,
            self.ci
        );

        // Send filtered cars to BlockSequencer
        self.send_tip_cut(filtered.clone()).await?;

        // Update per-lane watermark for the cars we just proposed
        for car in filtered.into_iter() {
            self.last_proposed_per_lane
                .insert(car.origin_node.clone(), car.n);
        }

        // Reset timer (batch proposer behavior)
        self.tip_cut_timer.reset();
        Ok(())
    }

    /// Helper to send the tip cut cars to the sequencer.
    async fn send_tip_cut(&mut self, cars: Vec<ProtoBlockCar>) -> Result<(), ()> {
        // Send to BlockSequencer which will:
        // 1. Compute digest and parent
        // 2. Send to BlockBroadcaster
        // 3. BlockBroadcaster wraps in AppendEntries and broadcasts to all nodes
        self.consensus_sequencer_tx.send(cars).await.map_err(|e| {
            error!("Failed to send tip cut to BlockSequencer: {:?}", e);
        })?;
        debug!("Sent tip cut to BlockSequencer for sequencing and broadcasting");
        Ok(())
    }

    /// Query lane_staging for the current tip cut.
    async fn query_tip_cut(&mut self) -> Result<Option<TipCut>, ()> {
        debug!("Querying LaneStaging for current tip cut");
        let (reply_tx, reply_rx) = oneshot::channel();

        // Send query
        self.lane_staging_query_tx
            .send(LaneStagingQuery::GetCurrentTipCut(reply_tx))
            .await
            .map_err(|e| {
                error!("Failed to send query to LaneStaging: {:?}", e);
            })?;

        // Wait for response
        reply_rx.await.map_err(|e| {
            error!("Failed to receive tip cut from LaneStaging: {:?}", e);
        })
    }
}
