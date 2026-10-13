use std::sync::mpsc::{sync_channel, channel, Receiver, Sender};
use std::thread;
use std::collections::BinaryHeap;

use crate::concurrency::context::{
    SimulationContext,
};

use crate::types::core::{
    Time,
    PortId,
};

use crate::wavepacket::wp::{
    TxPort,
    RxPort,
};

use crate::wavepacket::wp::WpBatch;

use crate::util::core::{Timed, Timable};

use timable_macro::Timable;


// Moved from generic_logic.rs


pub struct EntryPortHandle{
    tx: TxPort,
}


pub struct ExitPortHandle<'a, T: NodeHandle> {
    node_handle: &'a T,
    exit_port_id: PortId,
}

impl<'a, T: NodeHandle> ExitPortHandle<'a, T> {
    pub fn connect(&self, port: EntryPortHandle) {
        self.schedule_connect(port, 0);
    }
    pub fn schedule_connect(&self, port: EntryPortHandle, time: Time) {
        self.node_handle.get_control_channel().send(NodeControlEvent::Connect{
            exit_port_id: self.exit_port_id,
            tx_port: port.tx,
        }.timed(time)).unwrap();
    }
}

#[derive(Timable)]
pub enum NodeControlEvent<CustomControlEvent> {
    Start,
    Stop,
    Connect {
        exit_port_id: PortId,
        tx_port: TxPort,
    },
    Custom(CustomControlEvent),
}


// This strcut is world-facing. Even though non-idiomatic in rust, it should use the
// abstract base class trait pattern to warrant cross-node consistency and to reduce
// the amount of boilerplate code. A more idiomatic alternative would be the context
// struct pattern, but it is less viable here for the stated reason.
pub trait NodeHandle: Sized {
    type CustomControlEvent;
    type NodeTemplate;

    // user needs to implement this
    // new should register a new operator store
    fn new(ctx: SimulationContext, template: &Self::NodeTemplate, worker_id: u16, join_handle: std::thread::JoinHandle<()>, ports: Vec<TxPort>, control_channel: Sender<Timed<NodeControlEvent<Self::CustomControlEvent>>>) -> Self;
    fn get_tx_ports(&self) -> &Vec<TxPort>;
    fn get_control_channel(&self) -> &Sender<Timed<NodeControlEvent<Self::CustomControlEvent>>>;
    fn join(self);

    // everything else are derived
    fn get_tx_port(&self, id: PortId) -> TxPort {
        return self.get_tx_ports()[id as usize].clone();
    }
    fn entry_port(&self, id: PortId) -> EntryPortHandle {
        EntryPortHandle {
            tx: self.get_tx_ports()[id as usize].clone(),
        }
    }
    fn exit_port(&'_ self, id: PortId) -> ExitPortHandle<'_, Self> {
        ExitPortHandle::<Self>{
            node_handle: self,
            exit_port_id: id,
        }
    }

    fn start(&self) {
        self.schedule_start(0);
    }
    fn stop(&self) {
        self.schedule_stop(0);
    }
    fn send_node_control_event(&self, event: Self::CustomControlEvent) {
        self.schedule_node_control_event(event, 0);
    }

    fn schedule_start(&self, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Start.timed(time)).unwrap();
    }
    fn schedule_stop(&self, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Stop.timed(time)).unwrap();
    }
    fn schedule_node_control_event(&self, event: Self::CustomControlEvent, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Custom(event).timed(time)).unwrap();
    }
}


pub struct RunnerState<T: NodeWorker> {
    pub rx_ports: Vec<RxPort>,
    pub control_rx: Receiver<Timed<NodeControlEvent<T::CustomControlEvent>>>,
    pub control_event_queue: BinaryHeap<Timed<NodeControlEvent<T::CustomControlEvent>>>,
    pub time: Time,
}

impl<T: NodeWorker> RunnerState<T> {
    fn ctx<'a>(&'a mut self, global: &'a SimulationContext) -> RunnerContext<'a, T> {
        RunnerContext {
            runner: self,
            global,
        }
    }
}

pub struct NodeRunner<T: NodeWorker> {
    state: RunnerState<T>,
    worker: T,
}

pub trait NodeTemplate {
    fn rx_port_count(&self) -> usize;
}



pub trait NodeWorker: Send + Sized {
    type CustomControlEvent: Send;
    type NodeTemplate: NodeTemplate;
    type NodeHandle: NodeHandle<CustomControlEvent = Self::CustomControlEvent, NodeTemplate = Self::NodeTemplate>;

    fn new(ctx: SimulationContext, template: &Self::NodeTemplate) -> Self;
    fn handle_connection(&mut self, ctx: RunnerContext<Self>, exit_port_id: PortId, tx_port: TxPort);
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self>, custom_event: Self::CustomControlEvent);
    fn process_batch(&mut self, ctx: RunnerContext<Self>);
    fn id(&self) -> u16;
}


pub struct RunnerContext<'a, T: NodeWorker>{
    pub runner: &'a mut RunnerState<T>,
    pub global: &'a SimulationContext,
}


impl<T: NodeWorker + 'static> NodeRunner<T> {
    pub fn spawn(ctx: &SimulationContext, template: T::NodeTemplate) -> T::NodeHandle {
        let (tx_ports, rx_ports): (Vec<_>, Vec<_>) = (0..template.rx_port_count()).map(|_| {
            let (tx_raw, rx_raw) = sync_channel::<WpBatch>(3);
            let tx = TxPort {
                time: 0,
                tx: tx_raw,
            };
            let rx = RxPort {
                period_start: 0,
                period_end: 0,
                rx: rx_raw,
                // set the empty iterator
                current_period: Vec::new().into_iter().peekable(),
                current_time: 0,
            };
            (tx, rx)
        }).unzip();
        let (control_tx, control_rx) = channel::<Timed<NodeControlEvent<T::CustomControlEvent>>>();
        let worker = T::new(ctx.clone(), &template);
        let worker_id = worker.id();

        let mut runner = Self {
            state: RunnerState{
                rx_ports,
                control_rx,
                control_event_queue: BinaryHeap::new(),
                time: 0,
            },
            worker,
        };
        let ctx_clone = ctx.clone();
        let handle = thread::spawn(move || {
            runner.run(ctx_clone);
        });
        T::NodeHandle::new(ctx.clone(), &template, worker_id, handle, tx_ports, control_tx)
    }
    fn preload_control_events(&mut self){
        loop {
            let evt = self.state.control_rx.recv().unwrap();

            match evt.value {
                NodeControlEvent::Start => {
                    self.state.time = evt.time;
                    break;
                },
                _ => {
                    self.state.control_event_queue.push(evt);
                }
            }
        }
    }
    fn run(&mut self, ctx: SimulationContext){
        self.preload_control_events();
        loop {
            while let Ok(evt) = self.state.control_rx.try_recv() {
                self.state.control_event_queue.push(evt);
            }
            while self.state.control_event_queue.peek().is_some_and(|evt|evt.time <= self.state.time) {
                // pop_if is nightly, so we use a less rusty alternative with unwrap()
                let evt = self.state.control_event_queue.pop().unwrap();
                match evt.value {
                    NodeControlEvent::Start => {
                        panic!("double start is currently not supported");
                    },
                    NodeControlEvent::Stop => {
                        // let the thread join
                        return;
                    },
                    NodeControlEvent::Connect{exit_port_id, tx_port} => {
                        self.worker.handle_connection(self.state.ctx(&ctx), exit_port_id, tx_port);
                    },
                    NodeControlEvent::Custom(custom_event) => {
                        self.worker.handle_custom_event(self.state.ctx(&ctx), custom_event);
                    }
                }
            }
            self.worker.process_batch(self.state.ctx(&ctx));
        }

    }
}

