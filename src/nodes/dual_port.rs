#![allow(unused_imports)]
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender, SendError, RecvError};
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;

use nalgebra::{SMatrix, Complex};
use smallvec::SmallVec;

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
};
use crate::types::physics::{
    PhotonicKrausOperators,
};
use crate::util::set::U32OpenAddressSet;
use crate::nodes::dual_port_iterator::{
    DualPortIterator,
    DualPortCluster,
    PhotonicCluster,
};

pub type DualPortRunner = NodeRunner<DualPortWorker>;

pub enum DualPortEvent {}

// models 
pub struct DualPortWorker {
    sink_left: Option<TxPort>,
    sink_right: Option<TxPort>,
    seq: OpStoreHandle,
}

impl NodeWorker for DualPortWorker {
    type CustomControlEvent = DualPortEvent;
    type NodeTemplate = DualPortTemplate;
    type NodeHandle = DualPortWorkerHandle;

    fn new(template: &Self::NodeTemplate, seq: OpStoreHandle) -> Self {
        Self {
            sink_left: None,
            sink_right: None,
            seq,
        }
    }
    fn register_operator(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate) -> OpStoreHandle {
        ctx.operator_record.dual.add(template.scattering_matrix)
    }
    fn handle_connection(&mut self, ctx: RunnerContext<Self>, exit_port_id: PortId, tx_port: TxPort) {
        match exit_port_id {
            0 => {
                self.sink_left = Some(tx_port);
            },
            1 => {
                self.sink_right = Some(tx_port);
            },
            _ => {
                panic!("Dual-port node only has two exit ports addresses, 0 or 1.");
            }
        }
    }
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self>, custom_event: Self::CustomControlEvent) {
        match custom_event {}
    }
    fn process_batch(&mut self, ctx: RunnerContext<Self>) {
        let batch_policy = &ctx.global.config.load().batch;

        // NOTE: Needed to do disjoint borrow. Normal indexing doesn't work
        let (left_slice, right_slice) = ctx.runner.rx_ports.split_at_mut(1);
        let left_port = &mut left_slice[0];
        let right_port = &mut right_slice[0];
        // let left_port = &mut ctx.runner.rx_ports[0];
        // let right_port = &mut ctx.runner.rx_ports[1];

        let left_time = left_port.current_time;
        let right_time = right_port.current_time;
        let batch_constraint = batch_policy.get_constraint(left_time.min(right_time));

        let mut left_batch = left_port.get_batch(batch_constraint.clone());
        let mut right_batch = left_port.get_batch(batch_constraint.clone());
        let start_time = left_batch.start_time.min(right_batch.start_time);
        let end_time = left_batch.end_time.min(right_batch.end_time);

        // Temporal boundary condition. We don't want any packet clusters (island of interaction)
        // cross batch boundary
        //
        // Overlapping operator weights should add up to 1
        loop {
            let left_edge = left_batch.trailing_edge();
            while let Some(wp) = right_port.get_overlapping_or_before(left_edge) {
                right_batch.push(wp);
            }
            let right_edge = right_batch.trailing_edge();
            while let Some(wp) = left_port.get_overlapping_or_before(right_edge) {
                left_batch.push(wp);
            }
            if right_port.is_strictly_after(left_batch.trailing_edge()) {
                break;
            }
        }
        let mut slice = ctx.global.interaction_store.get_states(vec![&mut left_batch.batch, &mut right_batch.batch]);
        let mut sink_batch_left: Vec<WavePacket> = Vec::new();
        let mut sink_batch_right: Vec<WavePacket> = Vec::new();

        for cluster in DualPortIterator::new(left_batch, right_batch) {
            match cluster {
                // TODO: Merge these two branches
                DualPortCluster::LeftIsolate(wp_source) => {
                    ctx.runner.time = wp_source.time;
                    let state = slice.get_mut(wp_source.state_handle).unwrap_as_island_or_else(|_| {
                        panic!("Expected IslandOfInteraction, but got something else");
                    });
                    let mut wp_sink_left = wp_source.clone();
                    let mut wp_sink_right = wp_source.clone();
                    wp_sink_left.snowflake = snowflake::next_u32();
                    wp_sink_right.snowflake = snowflake::next_u32();
                    let source_mode = state.active_packets.extract(wp_source.snowflake);
                    let sink_mode_left = state.register_wavepacket(&wp_sink_left);
                    let sink_mode_right = state.register_wavepacket(&wp_sink_right);
                    state.operators.push(Operator::DualUnivariate{
                        store_handle: self.seq,
                        time: wp_source.time,
                        incidence_port_id: 0,
                        source_modes: [source_mode],
                        sink_modes: [sink_mode_left, sink_mode_right],
                    });
                    sink_batch_left.push(wp_sink_left);
                    sink_batch_right.push(wp_sink_right);
                },
                DualPortCluster::RightIsolate(wp_source) => {
                    ctx.runner.time = wp_source.time;
                    let state = slice.get_mut(wp_source.state_handle).unwrap_as_island_or_else(|_| {
                        panic!("Expected IslandOfInteraction, but got something else");
                    });
                    let mut wp_sink_left = wp_source.clone();
                    let mut wp_sink_right = wp_source.clone();
                    wp_sink_left.snowflake = snowflake::next_u32();
                    wp_sink_right.snowflake = snowflake::next_u32();
                    let source_mode = state.active_packets.extract(wp_source.snowflake);
                    let sink_mode_left = state.register_wavepacket(&wp_sink_left);
                    let sink_mode_right = state.register_wavepacket(&wp_sink_right);
                    state.operators.push(Operator::DualUnivariate{
                        store_handle: self.seq,
                        time: wp_source.time,
                        incidence_port_id: 1,
                        source_modes: [source_mode],
                        sink_modes: [sink_mode_left, sink_mode_right],
                    });
                    sink_batch_left.push(wp_sink_left);
                    sink_batch_right.push(wp_sink_right);
                },
                DualPortCluster::Cluster(mut cluster) => {
                    // TODO: Investigate correctness of this time definition
                    // This is not monotonically ascending, but it is good enough
                    // forn the coarse time keeping purposes
                    // In order to change the order we might need to reshuffle some
                    // wavepackets and clusters on the fly, which would warrant the use
                    // of intermediate buffering, which is at this point too costly
                    // to implement with unknown runtime overhead. Let's keep it simple.
                    ctx.runner.time = cluster.time().max(ctx.runner.time);
                    // merge everything
                    let state_handle = cluster.merge_states(&mut slice);
                    let state = slice.get_mut(state_handle).unwrap_as_island_or_else(|_| {
                        panic!("Expected IslandOfInteraction, but got something else");
                    });
                    for (left_index, right_index) in cluster.pairs.iter() {
                        let wp_source_left = &cluster.left_packets[*left_index as usize];
                        let wp_source_right = &cluster.right_packets[*right_index as usize];

                        let mut wp_sink_left_left = wp_source_left.clone();
                        let mut wp_sink_left_right = wp_source_left.clone();
                        let mut wp_sink_right_left = wp_source_right.clone();
                        let mut wp_sink_right_right = wp_source_right.clone();
                        wp_sink_left_left.snowflake = snowflake::next_u32();
                        wp_sink_left_right.snowflake = snowflake::next_u32();
                        wp_sink_right_left.snowflake = snowflake::next_u32();
                        wp_sink_right_right.snowflake = snowflake::next_u32();

                        let source_mode_left = state.active_packets.extract(wp_source_left.snowflake);
                        let source_mode_right = state.active_packets.extract(wp_source_right.snowflake);
                        let sink_mode_left_left =   state.register_wavepacket(&wp_sink_left_left);
                        let sink_mode_left_right =  state.register_wavepacket(&wp_sink_left_right);
                        let sink_mode_right_left =  state.register_wavepacket(&wp_sink_right_left);
                        let sink_mode_right_right = state.register_wavepacket(&wp_sink_right_right);
                        state.operators.push(Operator::DualBivariate{
                            store_handle: self.seq,
                            time: ctx.runner.time,
                            packet_indistinguishability: wp_source_left.indistinguishability(wp_source_right),
                            source_modes: [source_mode_left, source_mode_right],
                            sink_modes: [sink_mode_left_left, sink_mode_left_right, sink_mode_right_left, sink_mode_right_right],
                        });
                        sink_batch_left.push(wp_sink_left_left);
                        sink_batch_left.push(wp_sink_left_right);
                        sink_batch_right.push(wp_sink_right_left);
                        sink_batch_right.push(wp_sink_right_right);
                    }
                }
            }
        }

        if let Some(port) = &mut self.sink_left {
            port.send_batch(WpBatch{
                start_time,
                end_time,
                batch: sink_batch_left,
            }).unwrap();
        } else {
            panic!("Unconnected sink is currently UB (dual left)");
        }

        if let Some(port) = &mut self.sink_right {
            port.send_batch(WpBatch{
                start_time,
                end_time,
                batch: sink_batch_right,
            }).unwrap();
        } else {
            panic!("Unconnected sink is currently UB (dual right)");
        }
    }
}


pub struct DualPortWorkerHandle {
    pub ports: Vec<TxPort>,
    pub control: Sender<TimedControlEvent<DualPortEvent>>,
    pub seq: OpStoreHandle,// index in the operator store
    pub join_handle: std::thread::JoinHandle<()>,
}

pub struct DualPortTemplate {
    pub scattering_matrix: SMatrix<Complex<f32>, 4, 4>,
}

impl NodeHandle for DualPortWorkerHandle {
    type CustomControlEvent = DualPortEvent;
    type NodeTemplate = DualPortTemplate;

    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: OpStoreHandle, join_handle: std::thread::JoinHandle<()>, ports: Vec<TxPort>, control: Sender<TimedControlEvent<Self::CustomControlEvent>>) -> Self {
        Self {
            ports,
            control,
            seq,
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

impl DualPortWorkerHandle {
    // Human facing API (can be slow). ctx is cloned for now
    fn set_scattering_matrix(&self, ctx: Arc<SimulationContext>, scattering_matrix: SMatrix<Complex<f32>, 4, 4>, time: Time) {
        ctx.operator_record.dual.set(self.seq, scattering_matrix, time);
    }
}
