// use std::sync::Arc;
// use std::sync::mpsc::{sync_channel, SyncSender};
// use std::thread;
// 
// use smallvec::smallvec;
// 
// use crate::nodes::core::{BatchConstraint, RxPort, TxPort, WorkerHandle, WpBatch};
// use crate::concurrency::interaction_store::{
//     CollapseResult,
//     InteractionCell,
//     InteractionStore,
//     Operator,
//     WpResult,
// };
// 
// 
// // Superconducting nanowire single-photon detector. Terminal node: it
// // appends the SPD operator to the island, and once every packet of the
// // island has been detected it collapses the island into a CollapseResult
// // and retires the slot.
// // MOCK: no amplitude evaluation yet — every arriving packet produces a
// // deterministic click (time tag). The real collapse will walk the island's
// // operator DAG and evaluate it against the per-node Kraus/scatter operators
// // looked up by (node, time).
// pub struct SpdWorker {
//     port: RxPort,
//     id: u16,
//     store: Arc<InteractionStore>,
//     tags: SyncSender<(u16, u64)>,
//     batch_period: u64,
//     batch_size: usize,
// }
// 
// impl SpdWorker {
//     pub fn spawn(
//         store: Arc<InteractionStore>,
//         id: u16,
//         tags: SyncSender<(u16, u64)>,
//     ) -> WorkerHandle {
//         let (tx_raw, rx_raw) = sync_channel::<WpBatch>(3);
//         let tx = TxPort {
//             time: 0,
//             tx: tx_raw,
//         };
//         let rx = RxPort {
//             period_start: 0,
//             period_end: 0,
//             rx: rx_raw,
//             current_period: Vec::new().into_iter().peekable(),
//             current_time: 0,
//         };
//         let mut worker = Self {
//             port: rx,
//             id,
//             store,
//             tags,
//             batch_period: 20_000_000,
//             batch_size: 200,
//         };
//         thread::spawn(move || {
//             worker.run();
//         });
//         WorkerHandle {
//             ports: vec![tx],
//         }
//     }
//     fn run(&mut self) {
//         loop {
//             let mut batch = self.port.get_batch(BatchConstraint {
//                 timeout: self.port.current_time + self.batch_period,
//                 max_size: self.batch_size,
//             });
//             let mut slice = self.store.get_states(vec![&mut batch]);
//             for wp in batch.iter() {
//                 let island = match slice.get_mut(wp.state_handle) {
//                     InteractionCell::IslandOfInteraction(island) => island,
//                     _cell => {
//                         panic!("Expected IslandOfInteraction, but got something else");
//                     }
//                 };
//                 let (op_index, port_index) = island.active_packets.extract(wp.snowflake);
//                 let new_op_index = island.operators.len() as u16;
//                 island.operators.push(Operator::SPD {
//                     node: self.id,
//                     time: wp.time,
//                 });
//                 island.operators[op_index as usize].set_sink(port_index, (new_op_index, 0));
// 
//                 // last packet of the island detected: collapse and free the slot
//                 if island.active_packets.is_empty() {
//                     *slice.get_mut(wp.state_handle) = InteractionCell::Result(CollapseResult {
//                         packets: smallvec![WpResult::Success {
//                             time: wp.time,
//                             spd_id: self.id,
//                             slot_handle: wp.state_handle,
//                         }],
//                     });
//                     slice.retire(wp.state_handle);
//                 }
// 
//                 if self.tags.send((self.id, wp.time)).is_err() {
//                     // tag consumer hung up: tear down
//                     return;
//                 }
//             }
//         }
//     }
// }
