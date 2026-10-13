use std::sync::mpsc::{Sender};
use std::sync::{Arc};

use nalgebra::{SMatrix, Complex, matrix};

use crate::nodes::core::{
    NodeRunner,
    NodeHandle,
    NodeWorker,
    NodeTemplate,
    NodeControlEvent,
    RunnerContext,
};

use crate::util::core::{
    Timed,
};

use crate::wavepacket::wp::{
    WavePacket,
    WpBatch,
    TxPort,
};

use crate::concurrency::interaction_store::{
    Operator,
};
use crate::concurrency::context::{
    SimulationContext,
    OpStoreHandle,
};

use crate::types::core::{
    Time,
    PortId,
};

pub type DualPortRunner = NodeRunner<DualPortWorker>;

pub enum DualPortEvent {}

// models 
pub struct DualPortWorker {
    sink_left: Option<TxPort>,
    sink_right: Option<TxPort>,
    worker_id: OpStoreHandle,
}

impl NodeWorker for DualPortWorker {
    type CustomControlEvent = DualPortEvent;
    type NodeTemplate = DualPortTemplate;
    type NodeHandle = DualPortWorkerHandle;

    fn new(ctx: SimulationContext, template: &Self::NodeTemplate) -> Self {
        let worker_id = ctx.operator_record.dual.add(template.scattering_matrix);
        // TODO: Partially utilized. Implement bitflag and full recording
        ctx.telemetry.dual.push(crossbeam_utils::CachePadded::new(crate::concurrency::context::DualMetrics{
            base: crate::concurrency::context::BaseMetrics{
                packet_consumed: 0.into(),
                packet_generated: 0.into(),
                sim_time: 0.into(),
            },
            merged_states: 0.into(),
            multi_packet_event: 0.into(),
            pair_count: 0.into(),
            paired_packets: 0.into(),
            single_packets: 0.into(),
        }));
        Self {
            sink_left: None,
            sink_right: None,
            worker_id,
        }
    }
    fn id(&self) -> u16 {
        self.worker_id
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
        let mut end_time = left_batch.end_time.min(right_batch.end_time);

        // Temporal boundary condition. We don't want any packet clusters (island of interaction)
        // cross batch boundary
        //
        // Overlapping operator weights should add up to 1
        loop {
            let left_edge = left_batch.trailing_edge();
            while let Some(wp) = right_port.get_overlapping_or_before(left_edge) {
                if wp.time > end_time {
                    // TODO: Consider using leading edge for the time. Also need to update EPPS if
                    // we do.
                    end_time = wp.time;
                }
                right_batch.push(wp);
            }
            let right_edge = right_batch.trailing_edge();
            while let Some(wp) = left_port.get_overlapping_or_before(right_edge) {
                if wp.time > end_time {
                    end_time = wp.time;
                }
                left_batch.push(wp);
            }
            if right_port.is_strictly_after(left_batch.trailing_edge()) {
                break;
            }
        }
        let mut slice = ctx.global.interaction_store.get_states(vec![&mut left_batch.batch, &mut right_batch.batch]);

        ctx.global.telemetry.dual[self.worker_id.into()].base.packet_consumed.fetch_add((left_batch.len() + right_batch.len()) as u64, std::sync::atomic::Ordering::Relaxed);
        ctx.global.telemetry.dual[self.worker_id.into()].base.sim_time.store(end_time, std::sync::atomic::Ordering::Relaxed);


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
                        store_handle: self.worker_id,
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
                        store_handle: self.worker_id,
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
    pub control: Sender<Timed<NodeControlEvent<DualPortEvent>>>,
    pub worker_id: OpStoreHandle,// index in the operator store
    pub join_handle: std::thread::JoinHandle<()>,
}

pub struct DualPortTemplate {
    pub scattering_matrix: SMatrix<Complex<f32>, 4, 4>,
}

impl NodeTemplate for DualPortTemplate {
    fn rx_port_count(&self) -> usize {
        2
    }
}

pub struct DualPortBuilder<'a> {
    template: DualPortTemplate,
    ctx: &'a SimulationContext
}

impl<'a> DualPortBuilder<'a> {
    pub fn new(ctx: &'a SimulationContext) -> Self {
        // symmetric 50:50 beamsplitter, polarization preserving.
        // Mode basis: (left_H, left_V, right_H, right_V); the reflected path
        // picks up the i phase (BS ⊗ I_pol)
        let s = Complex::new(std::f32::consts::FRAC_1_SQRT_2, 0.0);
        let i = Complex::new(0.0, std::f32::consts::FRAC_1_SQRT_2);
        let z = Complex::new(0.0f32, 0.0);
        DualPortBuilder {
            template: DualPortTemplate {
                scattering_matrix: matrix![
                    s, z, i, z;
                    z, s, z, i;
                    i, z, s, z;
                    z, i, z, s;
                ],
            },
            ctx,
        }
    }
    pub fn scattering_matrix(mut self, scattering_matrix: SMatrix<Complex<f32>, 4, 4>) -> Self {
        self.template.scattering_matrix = scattering_matrix;
        self
    }
    pub fn spawn(self) -> DualPortWorkerHandle {
        DualPortRunner::spawn(self.ctx, self.template)
    }
}

impl NodeHandle for DualPortWorkerHandle {
    type CustomControlEvent = DualPortEvent;
    type NodeTemplate = DualPortTemplate;

    fn new(ctx: SimulationContext, template: &Self::NodeTemplate, worker_id: OpStoreHandle, join_handle: std::thread::JoinHandle<()>, ports: Vec<TxPort>, control: Sender<Timed<NodeControlEvent<Self::CustomControlEvent>>>) -> Self {
        Self {
            ports,
            control,
            worker_id,
            join_handle,
        }
    }
    fn get_tx_ports(&self) -> &Vec<TxPort>{
        &self.ports
    }
    fn get_control_channel(&self) -> &Sender<Timed<NodeControlEvent<Self::CustomControlEvent>>> {
        &self.control
    }
    fn join(self) {
        self.join_handle.join().unwrap();
    }
}

impl DualPortWorkerHandle {
    // Human facing API (can be slow). ctx is cloned for now
    fn set_scattering_matrix(&self, ctx: Arc<SimulationContext>, scattering_matrix: SMatrix<Complex<f32>, 4, 4>, time: Time) {
        ctx.operator_record.dual.set(self.worker_id, scattering_matrix, time);
    }
}
