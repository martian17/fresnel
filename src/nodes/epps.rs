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
    Operator,
};
use crate::concurrency::context::{
    SimulationContext,
    OpStoreHandle,
};

use crate::types::core::{
    PortAddress,
    Time,
    PortId,
    BatchConstraint,
    SinkModeLocation,
};
use crate::types::physics::{
    PhotonicKrausOperators,
};
use crate::concurrency::snowflake;

pub type EPPSRunner = NodeRunner<EPPSWorker>;

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
pub enum EPPSEvent {
    SetWaveProfile {
        port: PortId,
        profile: WaveProfile,
    },
    SetPumpFrequency(f64),
    SetSuccessProbability(f64),
    SyncTo(Time),
}


pub struct EPPSWorker {
    signal_profile: WaveProfile,
    idler_profile: WaveProfile,
    signal_sink: Option<TxPort>,
    idler_sink: Option<TxPort>,
    pump_period: f64,
    success_probability: f64,
    seq: OpStoreHandle,
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
    
    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: OpStoreHandle) -> Self {
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
    fn register_operator(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate) -> OpStoreHandle {
        ctx.operator_record.epps.add(template.density_matrix)
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
        // println!("EPPS processing batch");
        let start_time = ctx.runner.time;
        let batch_constraint = ctx.global.config.load().batch.get_constraint(ctx.runner.time);
        let mut pairs: Vec<(WavePacket, WavePacket)> = Vec::new();
        while ctx.runner.time <= batch_constraint.timeout && pairs.len() < batch_constraint.max_size {
            let bin_count = get_next_time_bin_count(self.success_probability);
            let dt = self.time_frac + self.pump_period * bin_count as f64;
            ctx.runner.time += dt.floor() as u64;
            self.time_frac = dt.rem_euclid(1.0);
            pairs.push((
                self.signal_profile.new_wave_packet(ctx.runner.time),
                self.idler_profile.new_wave_packet(ctx.runner.time),
            ));
        }
        let (mut slice, start_handle, end_handle) = ctx.global.interaction_store.create_states(pairs.len() as u32);
        let mut signal_packets: Vec<WavePacket> = Vec::new();
        let mut idler_packets: Vec<WavePacket> = Vec::new();
        for (i, (mut signal, mut idler)) in pairs.into_iter().enumerate() {
            // if we don't use indices, this will become double borrow, and we will have to copy the
            // indices, which is slow
            let handle = start_handle.wrapping_add(i as u32);
            signal.state_handle = handle;
            idler.state_handle = handle;
            let mut island = IslandOfInteraction::new();
            let signal_mode = island.register_wavepacket(&signal);
            let idler_mode = island.register_wavepacket(&idler);
            island.operators.push(Operator::EPPS{
                store_handle: self.seq,
                time: signal.time,
                source_modes: [],
                sink_modes: [
                        signal_mode,
                        idler_mode,
                ],
            });

            slice.set(handle, InteractionCell::IslandOfInteraction(island));
            signal_packets.push(signal);
            idler_packets.push(idler);
        }
        drop(slice);

        if let Some(signal_sink) = &mut self.signal_sink {
            signal_sink.send_batch(WpBatch{
                start_time,
                end_time: ctx.runner.time,
                batch: signal_packets,
            }).unwrap();
        } else {
            panic!("signal sink undefined. UB for now. gotta implement loss operator");
        }

        if let Some(idler_sink) = &mut self.idler_sink {
            idler_sink.send_batch(WpBatch{
                start_time,
                end_time: ctx.runner.time,
                batch: idler_packets,
            }).unwrap();
        } else {
            panic!("idler sink undefined. UB for now. gotta implement loss operator");
        }
    }
}

pub struct EPPSWorkerHandle {
    // no optical reception ports, just controls
    // pub control: Sender<EPPSControlEvent>,

    pub control: Sender<TimedControlEvent<EPPSEvent>>,
    pub seq: OpStoreHandle,// index in the operator store
    pub join_handle: std::thread::JoinHandle<()>,
}

impl NodeHandle for EPPSWorkerHandle {
    type CustomControlEvent = EPPSEvent;
    type NodeTemplate = EPPSTemplate;

    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: OpStoreHandle, join_handle: std::thread::JoinHandle<()>, _ports: Vec<TxPort>, control: Sender<TimedControlEvent<Self::CustomControlEvent>>) -> Self {
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
        self.join_handle.join().unwrap();
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
pub struct WaveProfile{
    pub time_sigma: u32,
    pub wavelength: f32,
    pub wavelength_sigma: f32,
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

pub struct EPPSTemplate {
    pub signal_profile: WaveProfile,
    pub idler_profile: WaveProfile,
    pub pump_frequency: f64,
    pub density_matrix: SMatrix<Complex<f32>, 4, 4>,
    pub success_probability: f64,
}
