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

    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: OpStoreHandle) -> Self {
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
        let mut right_batch = right_port.get_batch(batch_constraint.clone());
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

        // first pass: merge all the states
        {
            let mut left_index = 0;
            let mut right_index = 0;
            loop {
                let left = left_batch.get_mut(left_index);
                let right = right_batch.get_mut(right_index);
                let (early_batch, late_batch, early_index, late_index) = if left.is_none() && right.is_none() {
                    break;
                } else if left.is_some_and(|left| right.is_none_or(|right| left.time <= right.time)) {
                    (&mut left_batch, &mut right_batch, &mut left_index, &mut right_index)
                } else {
                    (&mut right_batch, &mut left_batch, &mut right_index, &mut left_index)
                };
                let early = early_batch.get_mut(*early_index).unwrap();
                slice.correct_state_handle_if_tombstone(early);
                for i in (*late_index)..late_batch.len() {
                    let late = late_batch.get_mut(i).unwrap();
                    if !early.overlaps(late) {
                        break;
                    }
                    slice.correct_state_handle_if_tombstone(late);
                    if early.state_handle != late.state_handle {
                        // merge late to early
                        slice.merge_islands(late.state_handle, early.state_handle);
                    }
                }
                *early_index += 1;
            }
        }
        // pass #2: update the sink packets and record the modes
        let mut sink_batch_left: Vec<WavePacket> = Vec::new();
        let mut sink_batch_right: Vec<WavePacket> = Vec::new();
        // (source mode, sink mode left, sink mode right)
        let mut mode_map_left: Vec<(u16, u16, u16)> = Vec::new();
        let mut mode_map_right: Vec<(u16, u16, u16)> = Vec::new();
        {
            let mut left_index = 0;
            let mut right_index = 0;
            loop {
                let left = left_batch.get_mut(left_index);
                let right = right_batch.get_mut(right_index);
                let (early_batch, late_batch, early_index, late_index, mode_map) = if left.is_none() && right.is_none() {
                    break;
                } else if left.is_some_and(|left| right.is_none_or(|right| left.time <= right.time)) {
                    (&mut left_batch, &mut right_batch, &mut left_index, &mut right_index, &mut mode_map_left)
                } else {
                    (&mut right_batch, &mut left_batch, &mut right_index, &mut left_index, &mut mode_map_right)
                };
                let source_wp = early_batch.get_mut(*early_index).unwrap();
                slice.correct_state_handle_if_tombstone(source_wp);
                let sink_left_wp = source_wp.clone().set_snowflake();
                let sink_right_wp = source_wp.clone().set_snowflake();

                let state = slice.get_mut(source_wp.state_handle).unwrap_as_island_or_else(|_| {
                    panic!("Expected island of interaction");
                });
                let early_mode = state.extract_wavepacket(source_wp);
                let sink_left_mode = state.register_wavepacket(&sink_left_wp);
                let sink_right_mode = state.register_wavepacket(&sink_right_wp);
                mode_map.push((early_mode, sink_left_mode, sink_right_mode));
                sink_batch_left.push(sink_left_wp);
                sink_batch_right.push(sink_right_wp);
                *early_index += 1;
            }
        }

        // pass #3: register operators for all the packets and pairs
        {
            let mut left_index = 0;
            let mut right_index = 0;
            let mut left_high_watermark = 0;
            let mut right_high_watermark = 0; 
            loop {
                let left = left_batch.get_mut(left_index);
                let right = right_batch.get_mut(right_index);
                let (early_batch, late_batch, early_index, late_index, early_high_watermark, late_high_watermark, mode_map_early, mode_map_late, incident_port) = if left.is_none() && right.is_none() {
                    break;
                } else if left.is_some_and(|left| right.is_none_or(|right| left.time <= right.time)) {
                    (&mut left_batch, &mut right_batch, &mut left_index, &mut right_index, &mut left_high_watermark, &mut right_high_watermark, &mut mode_map_left, &mut mode_map_right, 0)
                } else {
                    (&mut right_batch, &mut left_batch, &mut right_index, &mut left_index, &mut right_high_watermark, &mut left_high_watermark, &mut mode_map_right, &mut mode_map_left, 1)
                };
                let early = early_batch.get_mut(*early_index).unwrap();
                let state = slice.get_mut(early.state_handle).unwrap_as_island_or_else(|_| {
                    panic!("Every islands should have been merged in the previous pass.");
                });
                let mut is_univariate = early_high_watermark <= early_index;
                for i in (*late_index)..late_batch.len() {
                    let late = late_batch.get_mut(i).unwrap();
                    if !early.overlaps(late) {
                        break;
                    }
                    let (early_src, early_left, early_right) = mode_map_early[*early_index];
                    let (late_src, late_left, late_right) = mode_map_late[i];
                    is_univariate = false;
                    state.operators.push(Operator::DualBivariate{
                        store_handle: self.seq,
                        time: early.time,
                        packet_indistinguishability: early.indistinguishability(late),
                        source_modes: [early_src, late_src],
                        sink_modes: [early_left, early_right, late_left, late_right],
                    });
                    *late_high_watermark = i+1;
                }
                if is_univariate {
                    let (early_src, early_left, early_right) = mode_map_early[*early_index];
                    state.operators.push(Operator::DualUnivariate{
                        store_handle: self.seq,
                        time: early.time,
                        incidence_port_id: incident_port,
                        source_modes: [early_src],
                        sink_modes: [early_left, early_right],
                    });
                }
                *early_index += 1;
            }
        }
        drop(slice);

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
