#![allow(unused_imports)]
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender, SendError, RecvError};
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;

use nalgebra::{SMatrix, Complex};
use smallvec::{
    SmallVec,
    smallvec,
};
use rand;

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
    IslandOfInteraction,
    ActivePacketStore,
    Operator,
};
use crate::concurrency::context::SimulationContext;

use crate::types::core::{
    PortAddress,
    Time,
    NodeId,
    PortId,
    BatchConstraint,
};
use crate::types::physics::{
    PhotonicKrausOperators,
};
use crate::concurrency::snowflake;

// represents exponential distribution, which gives the waiting time that a
// given poisson process succeeds
// success prob: probability that a pair will be generated at any given pulse
fn get_next_time_bin_count(success_prob: f64) -> u64 {
    let u: f64 = rand::random();
    let k = (u.ln() / (1.0 - success_prob).ln()).floor() as u64;
    // k can be 0. If it is 0, it corresponds to multi photon event
    // for now, we keep them in the separate packet,
    // and not use the photon number basis encoding
    // for the sake of simplicity
    k
}


// TODO: Refine the multi-epps sync interraction, which should
// model the inter-source de-synchronization failure mode and
// the failure for white rabbit to establish inter-node synchronization
enum EPPSEvent {
    SetWaveProfile {
        port: PortId,
        profile: WaveProfile,
    },
    SetPumpFrequency(f64),
    SetSuccessProbability(f64),
    SyncTo(Time),
}


struct EPPSWorker {
    signal_profile: WaveProfile,
    idler_profile: WaveProfile,
    signal_sink: Option<TxPort>,
    idler_sink: Option<TxPort>,
    pump_period: f64,
    success_probability: f64,
    seq: NodeId,
    time_frac: f64,
}

impl EPPSWorker {
    fn pump_frequency(&self) -> f64 {
        1.0E+12/self.pump_period
    }
}

impl NodeWorker for EPPSWorker {
    type CustomControlEvent = EPPSEvent;
    type NodeTemplate = EPPSTemplate;
    type NodeHandle = EPPSWorkerHandle;
    
    fn new(template: &Self::NodeTemplate, seq: NodeId) -> Self {
        Self {
            signal_profile: template.signal_profile.clone(),
            idler_profile: template.idler_profile.clone(),
            signal_sink: None,
            idler_sink: None,
            pump_period: 1.0E+12/template.pump_frequency,
            success_probability: template.success_probability,
            time_frac: 0.0,
            seq,
        }
    }
    fn register_operator(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate) -> NodeId {
        ctx.operator_record.epps.add(template.density_matrix.clone())
    }
    fn handle_connection(&mut self, ctx: RunnerContext<Self>, exit_port_id: PortId, tx_port: TxPort) {
        match exit_port_id {
            0 => {
                self.signal_sink = Some(tx_port);
            },
            1 => {
                self.idler_sink = Some(tx_port);
            },
            _ => {
                panic!("EPPS node only supports exit port id of 0 or 1, corresponding to siganl and idler (Connection)");
            },
        }
    }
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self>, custom_event: Self::CustomControlEvent) {
        match custom_event {
            EPPSEvent::SetWaveProfile { port, profile } => {
                match port {
                    0 => {
                        self.signal_profile = profile;
                    },
                    1 => {
                        self.idler_profile = profile;
                    },
                    _ => {
                        panic!("EPPS node only supports exit port id of 0 or 1, corresponding to siganl and idler (WaveProfile)");
                    },
                }
            },
            EPPSEvent::SetPumpFrequency(frequency) => {
                self.pump_period = 1.0E+12/frequency;
            },
            EPPSEvent::SetSuccessProbability(prob) => {
                self.success_probability = prob;
            },
            EPPSEvent::SyncTo(time) => {
                let diff = time - ctx.runner.time;
                let n = (diff as f64/self.pump_period).round();
                self.time_frac = 0.0;
                ctx.runner.time = time + (self.pump_period * n) as u64;
            }
        }
    }
    fn process_batch(&mut self, ctx: RunnerContext<Self>) {
        let start_time = ctx.runner.time;
        let batch_constraint = ctx.global.config.load().batch.get_constraint(ctx.runner.time);
        let mut pairs: Vec<(WavePacket, WavePacket)> = Vec::new();
        while ctx.runner.time <= batch_constraint.timeout && pairs.len() < batch_constraint.max_size {
            let bin_count = get_next_time_bin_count(self.success_probability);
            let dt = self.time_frac + self.pump_period * bin_count as f64;
            ctx.runner.time = dt.floor() as u64;
            self.time_frac = dt.rem_euclid(1.0);
            pairs.push((
                self.signal_profile.new_wave_packet(ctx.runner.time),
                self.idler_profile.new_wave_packet(ctx.runner.time),
            ));
        }
        let mut slice = ctx.global.interaction_store.create_states(pairs.len() as u32);
        let mut signal_packets: Vec<WavePacket> = Vec::new();
        let mut idler_packets: Vec<WavePacket> = Vec::new();
        let mut i = 0;
        for (mut signal, mut idler) in pairs.into_iter() {
            // if we don't use indices, this will become double borrow, and we will have to copy the
            // indices, which is slow
            let handle = slice.get_handles()[i];
            signal.state_handle = handle;
            idler.state_handle = handle;
            let mut island = IslandOfInteraction::new();
            let _op_handle = island.add_operator(Operator::EPPS{
                node: self.seq,
                time: signal.time,
                // will be overwritten by the next operator
                sink_signal: (0, 0),
                sink_idler: (0, 0),
            });
            slice.set(handle, InteractionCell::IslandOfInteraction(island));
            signal_packets.push(signal);
            idler_packets.push(idler);
            i += 1;
        }
        if let Some(signal_sink) = &mut self.signal_sink {
            signal_sink.send_batch(WpBatch{
                start_time,
                end_time: ctx.runner.time,
                batch: signal_packets,
            });
        } else {
            panic!("signal sink undefined. UB for now. gotta implement loss operator");
        }

        if let Some(idler_sink) = &mut self.idler_sink {
            idler_sink.send_batch(WpBatch{
                start_time,
                end_time: ctx.runner.time,
                batch: idler_packets,
            });
        } else {
            panic!("idler sink undefined. UB for now. gotta implement loss operator");
        }
    }
}

struct EPPSWorkerHandle {
    // no optical reception ports, just controls
    // pub control: Sender<EPPSControlEvent>,

    pub control: Sender<TimedControlEvent<EPPSEvent>>,
    pub seq: NodeId,// index in the operator store
    pub join_handle: std::thread::JoinHandle<()>,
}

impl NodeHandle for EPPSWorkerHandle {
    type CustomControlEvent = EPPSEvent;
    type NodeTemplate = EPPSTemplate;

    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: NodeId, join_handle: std::thread::JoinHandle<()>, _ports: Vec<TxPort>, control: Sender<TimedControlEvent<Self::CustomControlEvent>>) -> Self {
        Self {
            control,
            seq,
            join_handle,
        }
    }
    fn get_tx_ports(&self) -> &Vec<TxPort>{
        panic!("NodeHandle has no input port");
    }
    fn get_control_channel(&self) -> &Sender<TimedControlEvent<Self::CustomControlEvent>> {
        &self.control
    }
    fn join(self) {
        self.join_handle.join();
    }
}

// TODO: Shove some of these in a trait
impl EPPSWorkerHandle {
    fn set_signal_wave_profile(&self, profile: WaveProfile, time: Time) {
        self.schedule_node_control_event(EPPSEvent::SetWaveProfile{
            port: 0,
            profile,
        }, time);
    }
    fn set_idler_wave_profile(&self, profile: WaveProfile, time: Time) {
        self.schedule_node_control_event(EPPSEvent::SetWaveProfile{
            port: 1,
            profile,
        }, time);
    }

    fn set_pump_frequency(&self, frequency: f64, time: Time) {
        self.schedule_node_control_event(EPPSEvent::SetPumpFrequency(frequency), time);
    }

    fn set_success_probability(&self, prob: f64, time: Time) {
        self.schedule_node_control_event(EPPSEvent::SetSuccessProbability(prob), time);
    }

    fn sync_to(&self, sync_to_time: u64) {
        self.send_node_control_event(EPPSEvent::SyncTo(sync_to_time));
    }

    fn set_density_matrix(&self, ctx: Arc<SimulationContext>, density_matrix: SMatrix<Complex<f32>, 4, 4>, time: Time) {
        ctx.operator_record.epps.set(self.seq, density_matrix, time);
    }
}


#[derive(Clone)]
struct WaveProfile{
    time_sigma: u32,
    wavelength: f32,
    wavelength_sigma: f32,
}

impl WaveProfile {
    fn new_wave_packet(&self, time: Time) -> WavePacket {
        WavePacket {
            time,
            time_sigma: self.time_sigma,
            wavelength: self.wavelength,
            wavelength_sigma: self.wavelength_sigma,
            state_handle: 0, // assigned later
            snowflake: snowflake::next_u32(),
        }
    }
}

struct EPPSTemplate {
    signal_profile: WaveProfile,
    idler_profile: WaveProfile,
    pump_frequency: f64,
    density_matrix: SMatrix<Complex<f32>, 4, 4>,
    success_probability: f64,
}


// // models 
// struct EPPSWorker {
//     // properties needed by every node worker
//     batch_period: u64,
//     batch_size: usize,
//     id: u16,
//     time: Time,
//     store: Arc<InteractionStore>,
//     control_channel: Receiver<EPPSControlEvent>,
//     control_event_queue: BinaryHeap<EPPSControlEvent>,
// 
//     // shape specific
//     signal_sink: Option<(PortAddress, TxPort)>,
//     idler_sink: Option<(PortAddress, TxPort)>,
// 
//     signal_profile: WaveProfile,
//     idler_profile: WaveProfile,
// 
//     pump_frequency: f64,
// }
// 
// 
// impl EPPSWorker {
//     pub fn spawn(
//         store: Arc<InteractionStore>,
//         id: u16,
//         template: EPPSTemplate,
//     ) -> EPPSWorkerHandle {
//         let (control_tx, control_rx) = channel::<EPPSControlEvent>();
//         let mut worker = Self {
//             batch_period: 20_000_000,
//             batch_size: 200,
//             id,
//             time: 0,
//             store,
//             control_channel: control_rx,
//             control_event_queue: BinaryHeap::new(),
// 
//             signal_sink: None,
//             idler_sink: None,
// 
//             signal_profile: template.signal_profile,
//             idler_profile: template.idler_profile,
// 
//             pump_frequency: template.pump_frequency,
//         };
//         thread::spawn(move ||{
//             worker.run();
//         });
//         EPPSWorkerHandle {
//             control: control_tx,
//         }
//     }
//     fn handle_control_event(&mut self) {
//         while let Ok(evt) = self.control_channel.try_recv() {
//             self.control_event_queue.push(evt);
//         }
//         while self.control_event_queue.peek().is_some_and(|evt|evt.time <= self.time) {
//             // pop_if is nightly, so we use a less rusty alternative with unwrap()
//             let evt = self.control_event_queue.pop().unwrap();
//             match evt.event_type {
//                 ControlEventType::ConnectSink {sink_tx, address} => {
//                     self.sink = Some((address, sink_tx));
//                 }
//             }
//         }
//     }
//     fn run(&mut self) {
//         loop {
//             self.handle_control_event();
//             // since it's a single port, no need to worry about boundary condition
//             // therefore we just get a batch here
//             let mut batch = self.port.get_batch(BatchConstraint{
//                 timeout: self.port.current_time + self.batch_period,
//                 max_size: self.batch_size,
//             });
//             let slice = self.store.get_states(vec![&mut batch.batch]);
//             let sink_batch: Vec<WavePacket> = Vec::new();
//             for wp in batch.batch {
//                 let state = match slice.get_mut(wp.state_handle) {
//                     InteractionCell::IslandOfInteraction(state) => state,
//                     _cell => {
//                         // TODO: Make this error nicer
//                         panic!("Expected IslandOfInteraction, but got something else"); 
//                     }
//                 };
//                 let sink_mode = state.active_packets.extract(wp.snowflake);
//                 let op_handle = state.add_operator(Operator::Single{
//                     node: self.id,
//                     time: wp.time,
//                     // these are placeholders
//                     sink: (0, 0),
//                 });
//                 state.set_sink(sink_mode, op_handle);
//             }
// 
//             if let Some((address, port)) = &mut self.sink {
//                 port.send_batch(WpBatch{
//                     start_time: batch.start_time + self.time_delay,
//                     end_time: batch.end_time + self.time_delay,
//                     batch: sink_batch,
//                 });
//             } else {
//                 // we don't handle this for now. We just let it leak
//                 // for wp in sink_batch {
//                 //     let state = match slice.get_mut(wp.state_handle) {
//                 //         InteractionCell::IslandOfInteraction(state) => state,
//                 //         _cell => {
//                 //             // TODO: Make this error nicer
//                 //             panic!("Expected IslandOfInteraction, but got something else"); 
//                 //         }
//                 //     };
//                 //     let op_handle = state.add_operator(Operator::Lost);
//                 //     let sink_mode = state.active_packets.extract(wp.snowflake);
//                 //     state.set_sink(sink_mode, op_handle);
//                 //     if state
//                 // }
//             }
//         }
//     }
// }


