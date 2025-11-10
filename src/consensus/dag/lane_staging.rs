/// Staging area for lane-based data dissemination.
/// Blocks are organized into lanes, each lane corresponding to a sender node.
/// Each block needs to be acknowledged by a quorum of nodes before being considered stable.

// TODO: Implement lane staging logic here.

// Listen for:
// - AppendBlocks from lane proposers (or self)
// - BlockAcks -> Acknowledgements from other nodes for blocks in lanes

// Send:
// - Block Acks

// Responsibilities:
// 1. Retrieve blocks from block receiver
// 2. Organize blocks into lanes based on sender (lane_logserver)
//     - If I receive a block for an occupied height in a lane, do nothing and don't acknowledge
// 3. Acknowledge Stored Blocks
//     - Send BlockAck messages to other nodes
// 4. Track acknowledgements for each block in each lane and CARs
//      - Once a block has enough acknowledgements, mark it as stable and forward to other nodes