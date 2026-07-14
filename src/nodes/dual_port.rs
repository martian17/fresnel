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
use crate::concurrency::context::SimulationContext;
use crate::concurrency::snowflake;

use crate::types::core::{
    PortAddress,
    Time,
    NodeId,
    PortId,
    BatchConstraint,
    SinkModeLocation,
};
use crate::types::physics::{
    PhotonicKrausOperators,
};
use crate::util::set::U32OpenAddressSet;

pub type DualPortRunner = NodeRunner<DualPortWorker>;

enum DualPortEvent {}

// models 
struct DualPortWorker {
    sink_left: Option<TxPort>,
    sink_right: Option<TxPort>,
    seq: NodeId,
}

impl NodeWorker for DualPortWorker {
    type CustomControlEvent = DualPortEvent;
    type NodeTemplate = DualPortTemplate;
    type NodeHandle = DualPortWorkerHandle;

    fn new(template: &Self::NodeTemplate, seq: NodeId) -> Self {
        Self {
            sink_left: None,
            sink_right: None,
            seq,
        }
    }
    fn register_operator(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate) -> NodeId {
        ctx.operator_record.dual.add(template.scattering_matrix.clone())
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
        let port_left = &mut ctx.runner.rx_ports[0];
        let port_right = &mut ctx.runner.rx_ports[1];

        let batch_constraint = batch_policy.get_constraint(port_left.current_time.min(port_right.current_time));

        let mut batch_left = port_left.get_batch(batch_constraint);
        let mut batch_right = port_left.get_batch(batch_constraint);

        // Temporal boundary condition. We don't want any packet clusters (island of interaction)
        // cross batch boundary
        loop {
            let left_edge = batch_left.trailing_edge();
            while let Some(wp) = port_right.get_overlapping_or_before(left_edge) {
                batch_right.push(wp);
            }
            let right_edge = batch_right.trailing_edge();
            while let Some(wp) = port_left.get_overlapping_or_before(right_edge) {
                batch_left.push(wp);
            }
            if port_right.is_strictly_after(batch_left.trailing_edge()) {
                break;
            }
        }
        let slice = ctx.global.interaction_store.get_states(vec![&mut batch_left.batch, &mut batch_right.batch]);
        let sink_left_batch: Vec<WavePacket> = Vec::new();
        let sink_right_batch: Vec<WavePacket> = Vec::new();


        let mut right_pivot: usize = 0;
        let mut right_last_overlap: usize = usize::MAX;

        for left_idx in 0..batch_left.len() {
            let mut left_packet = batch_left.batch[left_idx];
            let mut did_overlap = false;
            for right_idx in right_pivot..batch_right.len() {
                let mut right_packet = batch_right.batch[right_idx];
                if left_packet.overlaps(&right_packet) {
                    // overlap case
                    did_overlap = true;
                    right_last_overlap = right_idx;
                } else if right_packet.time < left_packet.time {
                    if right_idx > right_last_overlap {
                        // everything between last overlap and the left packet should be isolates
                        // right isolate

                    }
                    // tick pivot forward
                    right_pivot = right_idx + 1;
                } else {
                    // right pivot past left packet. Do nothing and early return
                    break;
                }
            }
            if !did_overlap {
                // left isolate
            }
        }

        loop {
            let mut left_packet = batch_left.batch[left_pivot];
            let mut right_packet = batch_right.batch[right_pivot];

            // left centered pivot



            if left_packet.overlaps(&right_packet) {
                if left_packet.state_handle != right_packet.state_handle {
                    // slice.correct_state_handle_if_tombstone(&mut left_packet);
                    slice.merge_islands(left_packet.state_handle, right_packet.state_handle);
                }
                let state = slice.get_mut(right_packet.state_handle);
                let 


                if left_packet.state_handle != right_packet.state_handle {
                    // left cell might be a tombstone, if it was merged, we need to trace
                    // until we find a live handle.
                    let mut left_handle = left_packet.state_handle;
                    let left_cell = while let InteractionCell::Tombstone(tombstone) = slice.get_mut(left_handle) {
                        left_handle = tombstone.move_destination;
                        tombstone.ref_cnt -= 1;
                        if tombstone.ref_cnt == 0 {
                        }
                    }
                    let left_cell = slice.get_mut(left_packet.state_handle);
                    let right_cell = slice.get_mut(right_packet.state_handle);
                    // WARNING: Unintuitive API. First argument is the target cell handle.
                    // This is necessary because IslandOfInteraction does not store the 
                    // handle to save space.
                    slice.merge_islands(left_packet.state_handle, right_packet.state_handle);
                }
                // at this point, left cell might be 
                let cell = slice.get_mut(right_packet.state_handle);
                let state = slice.get_mut(left_packet.state_handle).unwrap_as_island_or_else(|_| {
                    // TODO: Make this error nicer
                    panic!("Expected IslandOfInteraction, but got something else"); 
                });
                let left_previous_sink_port = 
            }
            break;

        }


        for wp in batch.batch {
            let state = match slice.get_mut(wp.state_handle) {
                InteractionCell::IslandOfInteraction(state) => state,
                _cell => {
                    // TODO: Make this error nicer
                    panic!("Expected IslandOfInteraction, but got something else"); 
                }
            };
            let sink_mode = state.active_packets.extract(wp.snowflake);
            let op_handle = state.add_operator(Operator::Dual{
                node: self.seq,
                time: wp.time,
                // these are placeholders. It will be replaced later using set_sink
                sink: (0, 0),
            });
            state.set_sink(sink_mode, op_handle);
        }

        if let Some(port) = &mut self.sink {
            port.send_batch(WpBatch{
                start_time: batch.start_time + self.delay,
                end_time: batch.end_time + self.delay,
                batch: sink_batch,
            });
        } else {
            panic!("Unconnected sink is currently UB");
            // we don't handle this for now. We just let it leak
            // for wp in sink_batch {
            //     let state = match slice.get_mut(wp.state_handle) {
            //         InteractionCell::IslandOfInteraction(state) => state,
            //         _cell => {
            //             // TODO: Make this error nicer
            //             panic!("Expected IslandOfInteraction, but got something else"); 
            //         }
            //     };
            //     let op_handle = state.add_operator(Operator::Lost);
            //     let sink_mode = state.active_packets.extract(wp.snowflake);
            //     state.set_sink(sink_mode, op_handle);
            //     if state
            // }
        }
    }
}


pub struct DualPortWorkerHandle {
    pub ports: Vec<TxPort>,
    pub control: Sender<TimedControlEvent<DualPortEvent>>,
    pub seq: NodeId,// index in the operator store
    pub join_handle: std::thread::JoinHandle<()>,
}

pub struct DualPortTemplate {
    scattering_matrix: SMatrix<Complex<f32>, 4, 4>,
}

impl NodeHandle for DualPortWorkerHandle {
    type CustomControlEvent = DualPortEvent;
    type NodeTemplate = DualPortTemplate;

    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: NodeId, join_handle: std::thread::JoinHandle<()>, ports: Vec<TxPort>, control: Sender<TimedControlEvent<Self::CustomControlEvent>>) -> Self {
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
        self.join_handle.join();
    }
}

impl DualPortWorkerHandle {
    // Human facing API (can be slow). ctx is cloned for now
    fn set_kraus_operators(&self, ctx: Arc<SimulationContext>, kraus_operators: PhotonicKrausOperators, time: Time) {
        ctx.operator_record.single.set(self.seq, kraus_operators, time);
    }
    fn set_delay(&self, delay: u64, time: Time) {
        self.schedule_node_control_event(DualPortEvent::SetDelay(delay), time);
    }
}
