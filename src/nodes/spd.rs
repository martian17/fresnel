use std::sync::mpsc::{sync_channel, channel, Sender, SyncSender};
use std::thread;

use rayon::prelude::*;

use crate::nodes::core::{
    WpBatch,
    TxPort,
    NodeRunner,
    NodeHandle,
    NodeWorker,
    NodeTemplate,
    TimedControlEvent,
    RunnerContext,
};
use crate::concurrency::interaction_store::{
    InteractionCell,
    Operator,
    CollapseResult,
};
use crate::concurrency::context::{
    SimulationContext,
};

use crate::types::core::{
    Time,
    PortId,
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
    worker_id: u16,
    spd_id: u16,
    dead_time: u64,
    dark_count: f64,
    efficiency: f64,
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

    fn new(ctx: SimulationContext, template: &Self::NodeTemplate) -> Self {
        let worker_id = ctx.telemetry.spd.push(crossbeam_utils::CachePadded::new(crate::concurrency::context::SPDMetrics{
            base: crate::concurrency::context::BaseMetrics{
                packet_consumed: 0.into(),
                packet_generated: 0.into(),
                sim_time: 0.into(),
            },
            clicks: 0.into(),
            dark_counts: 0.into(),
        })) as u16;
        let (tx, rx) = sync_channel::<WpBatch>(1);
        let (tx_settagger, rx_settagger) = channel::<SyncSender<TimeTagBatch>>();
        let dead_time = template.dead_time;
        let dark_count = template.dark_count;
        let efficiency = template.efficiency;
        Self {
            settagger_channel: tx_settagger,
            // tagger_channel: None,
            claimer_channel: Some(tx),
            claimer_thread_handle: Some(thread::spawn(move || {
                let mut tagger_channel = rx_settagger.try_recv().ok();
                let mut last_click: Time = 0;
                loop {
                    let mut batch = rx.recv().unwrap();
                    // polled after the batch recv: ConnectTimeTagger is handled on the
                    // node thread before its first process_batch, and that ordering
                    // carries over the channels — so polling here guarantees the very
                    // first batch of tags already sees the tagger
                    if let Ok(tagger_channel_value) = rx_settagger.try_recv() {
                        tagger_channel = Some(tagger_channel_value);
                    }
                    let mut result = ctx.interaction_store.claim_collapsed_packets(&mut batch.batch);
                    result.sort_unstable();
                    // apply the dead time, dark count, and efficiency
                    let mut filtered_result: Vec<u64> = Vec::new();
                    for time in result.iter() {
                        if rand::random_bool(1.0 - efficiency) {
                            continue;
                        }
                        if last_click + dead_time > *time {
                            continue;
                        }
                        let time_delta = (*time).saturating_sub(last_click);
                        // TODO: Use poisson process here
                        if rand::random_bool(time_delta as f64 / (1.0e12 / dark_count)) {
                            filtered_result.push(rand::random_range(last_click..*time));
                        }
                        filtered_result.push(*time);
                        last_click = *time;
                    }
                    let start_time = if let Some(min_time) = filtered_result.first() {
                        batch.start_time.min(*min_time)
                    } else {
                        batch.start_time
                    };
                    let end_time = if let Some(max_time) = filtered_result.last() {
                        batch.end_time.max(*max_time)
                    } else {
                        batch.end_time
                    };

                    // println!("Collapse ratio: {} -> {}", batch.batch.len(), result.len());
                    if let Some(tagger_channel) = &tagger_channel {
                        // sent even when no tag survived collapse: the batch's end_time
                        // still advances this channel's frontier in the merger
                        let tagger_result = tagger_channel.send(TimeTagBatch{
                            start_time,
                            end_time,
                            batch: filtered_result,
                        });
                        // TODO: Handle stop properly
                        if tagger_result.is_err() {
                            println!("SPD: target closed");
                            // pause this thread
                            thread::sleep(std::time::Duration::from_millis(10000));
                            return;
                        };
                    }
                }
            })),
            worker_id,
            spd_id: template.spd_id,
            dead_time,
            dark_count,
            efficiency,
        }
    }
    fn id(&self) -> u16 {
        self.worker_id
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
        ctx.global.telemetry.spd[self.worker_id.into()].base.packet_consumed.fetch_add(batch.batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        ctx.global.telemetry.spd[self.worker_id.into()].base.sim_time.store(batch.end_time, std::sync::atomic::Ordering::Relaxed);

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
    pub worker_id: u16,
    pub spd_id: u16,
}

pub struct SPDTemplate {
    pub spd_id: u16,
    pub dead_time: u64,
    pub dark_count: f64,
    pub efficiency: f64,
}

impl NodeTemplate for SPDTemplate {
    fn rx_port_count(&self) -> usize {
        1
    }
}

pub struct SPDBuilder<'a> {
    template: SPDTemplate,
    ctx: &'a SimulationContext
}

impl<'a> SPDBuilder<'a> {
    pub fn new(ctx: &'a SimulationContext) -> Self {
        SPDBuilder {
            template: SPDTemplate {
                spd_id: 0,
                dead_time: 20_000,// 20ns
                dark_count: 100.0, // 100 per second
                efficiency: 0.9,
            },
            ctx,
        }
    }
    pub fn id(mut self, id: u16) -> Self {
        self.template.spd_id = id;
        self
    }
    pub fn dead_time(mut self, dead_time: u64) -> Self {
        self.template.dead_time = dead_time;
        self
    }
    pub fn dark_count(mut self, dark_count: f64) -> Self {
        self.template.dark_count = dark_count;
        self
    }
    pub fn efficiency(mut self, efficiency: f64) -> Self {
        self.template.efficiency = efficiency;
        self
    }
    pub fn spawn(self) -> SPDWorkerHandle {
        SPDRunner::spawn(self.ctx, self.template)
    }
}

impl NodeHandle for SPDWorkerHandle {
    type CustomControlEvent = SPDEvent;
    type NodeTemplate = SPDTemplate;

    fn new(ctx: SimulationContext, template: &Self::NodeTemplate, worker_id: u16, join_handle: std::thread::JoinHandle<()>, ports: Vec<TxPort>, control: Sender<TimedControlEvent<Self::CustomControlEvent>>) -> Self {
        Self {
            ports,
            control,
            join_handle,
            worker_id,
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
