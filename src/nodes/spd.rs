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
use crate::parquet::core::TimeTagBatch;

pub type SPDRunner = NodeRunner<SPDWorker>;

pub enum SPDEvent {
    ConnectTimeTagger(SyncSender<TimeTagBatch>)
}

// models
pub struct SPDWorker {
    settagger_channel: Sender<SyncSender<TimeTagBatch>>,
    // tagger_channel: Option<SyncSender<Vec<Time>>>,
    claimer_channel: Option<SyncSender<WpBatch>>,
    claimer_thread_handle: Option<thread::JoinHandle<()>>,
    spd_id: u16,
    // wall-clock throughput instrumentation; read by the monitor thread in main
    packet_counter: Arc<std::sync::atomic::AtomicU64>,
    // latest simulated time (ps) seen by this SPD; lets the monitor compute
    // simulation speed as a fraction of real time
    sim_time_ps: Arc<std::sync::atomic::AtomicU64>,
}

impl Drop for SPDWorker {
    fn drop(&mut self) {
        drop(self.claimer_channel.take());          // hang up FIRST
        if let Some(h) = self.claimer_thread_handle.take() {
            drop(h.join());
        }
    }
}

impl NodeWorker for SPDWorker {
    type CustomControlEvent = SPDEvent;
    type NodeTemplate = SPDTemplate;
    type NodeHandle = SPDWorkerHandle;

    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: OpStoreHandle) -> Self {
        let (tx, rx) = sync_channel::<WpBatch>(1);
        let (tx_settagger, rx_settagger) = channel::<SyncSender<TimeTagBatch>>();
        Self {
            settagger_channel: tx_settagger,
            // tagger_channel: None,
            claimer_channel: Some(tx),
            claimer_thread_handle: Some(thread::spawn(move || {
                let mut tagger_channel = rx_settagger.try_recv().ok();
                loop {
                    let mut batch = rx.recv().unwrap();
                    // polled after the batch recv: ConnectTimeTagger is handled on the
                    // node thread before its first process_batch, and that ordering
                    // carries over the channels — so polling here guarantees the very
                    // first batch of tags already sees the tagger
                    if let Ok(tagger_channel_value) = rx_settagger.try_recv() {
                        tagger_channel = Some(tagger_channel_value);
                    }
                    let result = ctx.interaction_store.claim_collapsed_packets(&mut batch.batch);
                    // println!("Collapse ratio: {} -> {}", batch.batch.len(), result.len());
                    if let Some(tagger_channel) = &tagger_channel {
                        // sent even when no tag survived collapse: the batch's end_time
                        // still advances this channel's frontier in the merger
                        tagger_channel.send(TimeTagBatch{
                            start_time: batch.start_time,
                            end_time: batch.end_time,
                            batch: result,
                        }).unwrap();
                    }
                }
            })),
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
                self.settagger_channel.send(tagger_channel).unwrap();
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

        for wp_source in batch.batch.iter() {
            let state = slice.get_mut(wp_source.state_handle).unwrap_as_island_or_else(|_| {
                panic!("Expected the cell to be an island");
            });
            // NOTE: Do not extract
            // island should keep the active packets in case it gets merged or moved.
            let source_mode = state.get_wavepacket_mode(wp_source);
            state.terminated_packet_count += 1;
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
            if state.is_collapse_ready() {
                rayon_handles.push(handle);
                rayon_states.push(state.clone());
                // *cell = InteractionCell::ComputeWip;
            }
        }
        drop(slice);
        let mut rayon_slice = ctx.global.interaction_store.get_states_from_raw_handles(&rayon_handles);
        let rayon_results: Vec<CollapseResult> = rayon_states.par_iter().map(|state|{
            crate::collapser::monte_carlo::collapse(state)
            // let collapsed_packets: SmallVec<[WpResult; 20]> = state.operators.iter().filter_map(|op| match op {
            //     Operator::SPD{id, time, wp_snowflake, ..} => Some(WpResult::Success{
            //         time: *time,
            //         spd_id: *id,
            //         wp_snowflake: *wp_snowflake,
            //     }),
            //     _ => None,
            // }).collect();
            // let ref_cnt: usize = collapsed_packets.len();
            // CollapseResult {
            //     packets: collapsed_packets,
            //     ref_cnt,
            // }
        }).collect();
        for (result, handle) in rayon_results.into_iter().zip(rayon_handles.iter().copied()) {
            rayon_slice.set(handle, InteractionCell::Result(result));
        }
        drop(rayon_slice);

        // now wait for other threads to finish their results
        if let Some(channel) = &self.claimer_channel {
            channel.send(batch).unwrap();
        }
    }
}


pub struct SPDWorkerHandle {
    pub ports: Vec<TxPort>,
    pub control: Sender<TimedControlEvent<SPDEvent>>,
    pub join_handle: std::thread::JoinHandle<()>,
    pub spd_id: u16,
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
            spd_id: template.spd_id,
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
    pub fn connect_time_tagger(&self, channel: SyncSender<TimeTagBatch>) {
        self.schedule_node_control_event(SPDEvent::ConnectTimeTagger(channel), 0);
    }
}
