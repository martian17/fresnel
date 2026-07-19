#![allow(unused_imports)]
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender, SendError, RecvError};
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;

use nalgebra::{SMatrix, Complex};
use smallvec::SmallVec;

use std::sync::mpsc;
use rayon::prelude::*;

use crate::nodes::core::{
    WavePacket,
    WpBatch,
    TxPort,
    RxPort,
    // WorkerHandle,
    NodeRunner,
    NodeHandle,
    NodeWorker,
    TimedControlEvent,
    RunnerContext,
};
use crate::concurrency::interaction_store::{
    InteractionStore,
    InteractionCell,
    Operator,
    CollapseResult,
    WpResult,
};
use crate::concurrency::context::{
    SimulationContext,
    OpStoreHandle,
};
use crate::concurrency::snowflake;

use crate::types::core::{
    PortAddress,
    Time,
    PortId,
    BatchConstraint,
    SinkModeLocation,
    NormalizedTimeTag,
};
use crate::types::physics::{
    PhotonicKrausOperators,
};

pub type SPDRunner = NodeRunner<SPDWorker>;

pub enum SPDEvent {
    ConnectTimeTagger(SyncSender<Vec<Time>>)
}

// models
pub struct SPDWorker {
    tagger_channel: Option<SyncSender<Vec<Time>>>,
    spd_id: u16,
    // wall-clock throughput instrumentation; read by the monitor thread in main
    packet_counter: Arc<std::sync::atomic::AtomicU64>,
    // latest simulated time (ps) seen by this SPD; lets the monitor compute
    // simulation speed as a fraction of real time
    sim_time_ps: Arc<std::sync::atomic::AtomicU64>,
}

impl NodeWorker for SPDWorker {
    type CustomControlEvent = SPDEvent;
    type NodeTemplate = SPDTemplate;
    type NodeHandle = SPDWorkerHandle;

    fn new(template: &Self::NodeTemplate, seq: OpStoreHandle) -> Self {
        Self {
            tagger_channel: None,
            spd_id: template.spd_id,
            packet_counter: template.packet_counter.clone(),
            sim_time_ps: template.sim_time_ps.clone(),
        }
    }
    fn register_operator(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate) -> OpStoreHandle {
        // NOTE: SPD doesn't (shouldn't) register any operators
        // So we return a dummy value to conform to the trait shape
        0
    }
    fn handle_connection(&mut self, ctx: RunnerContext<Self>, exit_port_id: PortId, tx_port: TxPort) {
        panic!("SPD does not have any exit port. Use .connect_time_tagger()");
    }
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self>, custom_event: Self::CustomControlEvent) {
        match custom_event {
            SPDEvent::ConnectTimeTagger(tagger_channel) => {
                self.tagger_channel = Some(tagger_channel);
            },
        }
    }
    fn process_batch(&mut self, ctx: RunnerContext<Self>) {
        let port = &mut ctx.runner.rx_ports[0];
        let batch_policy = &ctx.global.config.load().batch;
        let mut batch = port.get_batch(batch_policy.get_constraint(port.current_time));
        self.packet_counter.fetch_add(batch.batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.sim_time_ps.store(batch.end_time, std::sync::atomic::Ordering::Relaxed);

        let mut slice = ctx.global.interaction_store.get_states(vec![&mut batch.batch]);
        let mut sink_batch: Vec<WavePacket> = Vec::new();

        for wp_source in batch.batch.iter() {
            let state = slice.get_mut(wp_source.state_handle).unwrap_as_island_or_else(|_| {
                panic!("Expected the cell to be an island");
            });
            let source_mode = state.extract_wavepacket(&wp_source);
            state.operators.push(Operator::SPD{
                id: self.spd_id,
                wp_snowflake: wp_source.snowflake,
                time: wp_source.time,
                source_modes: [source_mode],
                sink_modes: [],
            });
        }

        let mut rayon_handles = Vec::new();
        let mut rayon_states = Vec::new();
        for handle in slice.get_handles().iter() {
            let cell = slice.get_mut(handle);
            let state = cell.unwrap_as_island_or_else(|_| {
                panic!("Expected the cell to be an island");
            });
            if state.has_no_active_packets() {
                rayon_handles.push(handle);
                rayon_states.push(state.clone())
                // *cell = InteractionCell::ComputeWip;
            }
        }
        drop(slice);
        let mut rayon_slice = ctx.global.interaction_store.get_states_from_raw_handles(&rayon_handles);
        let rayon_results: Vec<CollapseResult> = rayon_states.par_iter().map(|state|{
            CollapseResult {
                packets: state.operators.iter().filter_map(|op| match op {
                    Operator::SPD{id, time, wp_snowflake, ..} => Some(WpResult::Success{
                        time: *time,
                        spd_id: *id,
                        wp_snowflake: *wp_snowflake,
                    }),
                    _ => None,
                }).collect(),
            }
        }).collect();
        for (result, handle) in rayon_results.into_iter().zip(rayon_handles.iter().copied()) {
            rayon_slice.set(handle, InteractionCell::Result(result));
        }
        drop(rayon_slice);

        // now wait for other threads to finish their results
        let result = ctx.global.interaction_store.get_collapsed_packets(&batch.batch);
        // println!("Got a batch of {} timetags. time: [{} {}]", batch.len(), batch.start_time, batch.end_time);
    }
}


pub struct SPDWorkerHandle {
    pub ports: Vec<TxPort>,
    pub control: Sender<TimedControlEvent<SPDEvent>>,
    pub join_handle: std::thread::JoinHandle<()>,
}

pub struct SPDTemplate {
    pub spd_id: u16,
    pub packet_counter: Arc<std::sync::atomic::AtomicU64>,
    pub sim_time_ps: Arc<std::sync::atomic::AtomicU64>,
}

impl NodeHandle for SPDWorkerHandle {
    type CustomControlEvent = SPDEvent;
    type NodeTemplate = SPDTemplate;

    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: OpStoreHandle, join_handle: std::thread::JoinHandle<()>, ports: Vec<TxPort>, control: Sender<TimedControlEvent<Self::CustomControlEvent>>) -> Self {
        Self {
            ports,
            control,
            join_handle,
        }
    }
    fn get_tx_ports(&self) -> &Vec<TxPort>{
        &self.ports
    }
    fn get_control_channel(&self) -> &Sender<TimedControlEvent<Self::CustomControlEvent>> {
        &self.control
    }
    fn join(self) {
        self.join_handle.join().unwrap();
    }
}

impl SPDWorkerHandle {
    // Human facing API (can be slow). ctx is cloned for now
    fn connect_time_tagger(&self, ctx: Arc<SimulationContext>, channel: SyncSender<Vec<Time>>) {
        self.schedule_node_control_event(SPDEvent::ConnectTimeTagger(channel), 0);
    }
}


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
//                 let (op_index, port_index) = island.extract_wavepacket(&wp);
//                 let new_op_index = island.operators.len() as u16;
//                 island.operators.push(Operator::SPD {
//                     node: self.id,
//                     time: wp.time,
//                 });
//                 island.operators[op_index as usize].set_sink(port_index, (new_op_index, 0));
// 
//                 // last packet of the island detected: collapse and free the slot
//                 if island.has_no_active_packets() {
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
