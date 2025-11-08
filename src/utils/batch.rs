use crate::proto::execution::ProtoTransaction;
use crate::rpc::server::MsgAckChan;
use crate::rpc::SenderType;

pub type RawBatch = Vec<ProtoTransaction>;

pub type MsgAckChanWithTag = (
    MsgAckChan,
    u64,        /* client tag */
    SenderType, /* client name */
);

pub type TxWithAckChanTag = (Option<ProtoTransaction>, MsgAckChanWithTag);
