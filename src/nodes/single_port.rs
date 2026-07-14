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

pub type SinglePortRunner = NodeRunner<SinglePortWorker>;

enum SinglePortEvent {
    SetDelay(u64),
}

// models 
struct SinglePortWorker {
    sink: Option<TxPort>,
    // max u32 picosecond time corresponds to 4ms, which is about 1200km in vacuum distance
    // which is still not out of the realm of possibility, especially with satellite based
    // communication, so we still use u64 here
    delay: u64,
    seq: NodeId,
}

impl NodeWorker for SinglePortWorker {
    type CustomControlEvent = SinglePortEvent;
    type NodeTemplate = SinglePortTemplate;
    type NodeHandle = SinglePortWorkerHandle;

    fn new(template: &Self::NodeTemplate, seq: NodeId) -> Self {
        Self {
            sink: None,
            delay: template.delay,
            seq,
        }
    }
    fn register_operator(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate) -> NodeId {
        ctx.operator_record.single.add(template.kraus_operators.clone())
    }
    fn handle_connection(&mut self, ctx: RunnerContext<Self>, exit_port_id: PortId, tx_port: TxPort) {
        if exit_port_id != 0 {
            panic!("Single-port node only supports exit port id of 0");
        }
        self.sink = Some(tx_port);
    }
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self>, custom_event: Self::CustomControlEvent) {
        match custom_event {
            SinglePortEvent::SetDelay(delay) => {
                self.delay = delay;
            },
        }
    }
    fn process_batch(&mut self, ctx: RunnerContext<Self>) {
        let port = &mut ctx.runner.rx_ports[0];
        let batch_policy = &ctx.global.config.load().batch;
        let mut batch = port.get_batch(batch_policy.get_constraint(port.current_time));

        let slice = ctx.global.interaction_store.get_states(vec![&mut batch.batch]);
        let mut sink_batch: Vec<WavePacket> = Vec::new();
        for wp in batch.batch {
            ctx.runner.time = wp.time;
            let state = slice.get_mut(wp.state_handle).unwrap_as_island_or_else(|_| {
                // TODO: Make this error nicer
                panic!("Expected IslandOfInteraction, but got something else"); 
            });
            let sink_mode = state.active_packets.extract(wp.snowflake);
            let op_handle = state.add_operator(Operator::Single{
                node: self.seq,
                time: wp.time,
                // these are placeholders. It will be replaced later using set_sink
                sink: (0, 0),
            });
            state.set_sink(sink_mode, op_handle);
            let sink_packet_snowflake = snowflake::next_u32();
            sink_batch.push(WavePacket{
                time: wp.time + self.delay,
                // TODO: Consider adding dispersion, and start thinking about
                // how to model chromatic abberation
                time_sigma: wp.time_sigma,
                wavelength: wp.wavelength,
                wavelength_sigma: wp.wavelength_sigma,
                state_handle: wp.state_handle,
                snowflake: sink_packet_snowflake,
            });
            state.active_packets.push(sink_packet_snowflake, SinkModeLocation{
                operator: op_handle,
                // single port only has single exit mode
                // and therefore mode index is always 0
                mode: 0,
            });
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


pub struct SinglePortWorkerHandle {
    pub ports: Vec<TxPort>,
    pub control: Sender<TimedControlEvent<SinglePortEvent>>,
    pub seq: NodeId,// index in the operator store
    pub join_handle: std::thread::JoinHandle<()>,
}

pub struct SinglePortTemplate {
    // single term with vacuum identity equals to jones matrix
    kraus_operators: PhotonicKrausOperators,
    delay: u64,
}

impl NodeHandle for SinglePortWorkerHandle {
    type CustomControlEvent = SinglePortEvent;
    type NodeTemplate = SinglePortTemplate;

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

impl SinglePortWorkerHandle {
    // Human facing API (can be slow). ctx is cloned for now
    fn set_kraus_operators(&self, ctx: Arc<SimulationContext>, kraus_operators: PhotonicKrausOperators, time: Time) {
        ctx.operator_record.single.set(self.seq, kraus_operators, time);
    }
    fn set_delay(&self, delay: u64, time: Time) {
        self.schedule_node_control_event(SinglePortEvent::SetDelay(delay), time);
    }
}
