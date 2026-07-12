#![allow(unused_imports)]
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender, SendError, RecvError};
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::sync::mpsc;

use crate::nodes::core::{
    WavePacket,
    WpBatch,
    TxPort,
    RxPort,
    // WorkerHandle,
    Connection,
    BatchConstraint,
    ControlEvent,
    ControlEventType,
};
use crate::concurrency::interaction_store::{
    InteractionStore,
    InteractionCell,
    Operator,
};

use crate::types::core::{
    PortAddress,
    Time,
    PortId,
    NodeId,
};



struct EntryPortHandle{
    tx: TxPort,
}


struct ExitPortHandle<'a, T: NodeHandle> {
    node_handle: &'a T,
    exit_port_id: PortId,
}

impl<'a, T: NodeHandle> ExitPortHandle<'a, T> {
    fn connect(&self, port: EntryPortHandle) {
        self.schedule_connect(port, 0);
    }
    fn schedule_connect(&self, port: EntryPortHandle, time: Time) {
        self.node_handle.get_control_channel().send(NodeControlEvent::Connect{
            exit_port_id: self.exit_port_id,
            tx_port: port.tx,
        }.timed(time));
    }
}

enum NodeControlEvent<CustomControlEvent> {
    Start,
    Stop,
    Connect {
        exit_port_id: PortId,
        tx_port: TxPort,
    },
    Custom(CustomControlEvent),
}

impl <CustomControlEvent> NodeControlEvent<CustomControlEvent> {
    fn timed(self, time: Time) -> TimedControlEvent<CustomControlEvent>{
        TimedControlEvent::<CustomControlEvent> {
            time,
            event: self,
        }
    }
}

struct TimedControlEvent<CustomControlEvent> {
    time: Time,
    event: NodeControlEvent<CustomControlEvent>
}

impl <T>PartialEq for TimedControlEvent<T> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl <T>Eq for TimedControlEvent<T> {}

impl <T>Ord for TimedControlEvent<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse the standard comparison to create a min-heap
        other.time.cmp(&self.time) 
    }
}

impl <T>PartialOrd for TimedControlEvent<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// This strcut is world-facing. Even though non-idiomatic in rust, it should use the
// abstract base class trait pattern to warrant cross-node consistency and to reduce
// the amount of boilerplate code. A more idiomatic alternative would be the context
// struct pattern, but it is less viable here for the stated reason.
trait NodeHandle: Sized {
    type CustomControlEvent;
    type NodeTemplate;

    // user needs to implement this
    fn get_tx_ports(&self) -> &Vec<TxPort>;
    fn get_control_channel(&self) -> &Sender<TimedControlEvent<Self::CustomControlEvent>>;
    fn get_node_id(&self) -> NodeId;
    fn join(&self);
    fn new(template: &Self::NodeTemplate, join_handle: std::thread::JoinHandle<()>) -> Self;

    // everything else are derived
    fn get_tx_port(&self, id: PortId) -> TxPort {
        return self.get_tx_ports()[id as usize].clone();
    }
    fn entry_port(&self, id: PortId) -> EntryPortHandle {
        EntryPortHandle {
            tx: self.get_tx_ports()[id as usize].clone(),
        }
    }
    fn exit_port(&self, id: PortId) -> ExitPortHandle<Self> {
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
        self.get_control_channel().send(NodeControlEvent::Start.timed(time));
    }
    fn schedule_stop(&self, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Stop.timed(time));
    }
    fn schedule_node_control_event(&self, event: Self::CustomControlEvent, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Custom(event).timed(time));
    }
}

struct SimulationContext {
    interaction_store: InteractionStore,
}

struct RunnerState<T: NodeWorker> {
    rx_ports: Vec<RxPort>,
    control_rx: Receiver<TimedControlEvent<T::CustomControlEvent>>,
    control_event_queue: BinaryHeap<TimedControlEvent<T::CustomControlEvent>>,
    time: Time,
}

impl<T: NodeWorker> RunnerState<T> {
    fn ctx<'a>(&'a mut self, global: &'a Arc<SimulationContext>) -> RunnerContext<'a, T> {
        RunnerContext {
            runner: self,
            global,
        }
    }
}

struct NodeRunner<T: NodeWorker> {
    state: RunnerState<T>,
    worker: T,
}



trait NodeWorker: Send + Sized {
    type CustomControlEvent: Send;
    type NodeTemplate;
    type NodeHandle: NodeHandle<CustomControlEvent = Self::CustomControlEvent, NodeTemplate = Self::NodeTemplate>;

    fn new(template: &Self::NodeTemplate) -> Self;
    fn handle_connection(&mut self, ctx: RunnerContext<Self>, exit_port_id: PortId, tx_port: TxPort);
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self>, custom_event: Self::CustomControlEvent);
    fn process_batch(&mut self, ctx: RunnerContext<Self>);

}


struct RunnerContext<'a, T: NodeWorker>{
    runner: &'a mut RunnerState<T>,
    global: &'a Arc<SimulationContext>,
}


impl<T: NodeWorker + 'static> NodeRunner<T> {
    fn spawn(ctx: Arc<SimulationContext>, rx_port_count: usize, template: T::NodeTemplate) -> T::NodeHandle {
        let (tx_ports, rx_ports): (Vec<_>, Vec<_>) = (0..rx_port_count).map(|_| {
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
        let (control_tx, control_rx) = channel::<TimedControlEvent<T::CustomControlEvent>>();

        let mut runner = Self {
            state: RunnerState{
                rx_ports,
                control_rx,
                control_event_queue: BinaryHeap::new(),
                time: 0,
            },
            worker: T::new(&template),
        };
        let handle = thread::spawn(move || {
            runner.run(ctx);
        });
        T::NodeHandle::new(&template, handle)
    }
    fn preload_control_events(&mut self){
        loop {
            let evt = self.state.control_rx.recv().unwrap();

            match evt.event {
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
    fn run(&mut self, ctx: Arc<SimulationContext>){
        self.preload_control_events();
        loop {
            while let Ok(evt) = self.state.control_rx.try_recv() {
                self.state.control_event_queue.push(evt);
            }
            while self.state.control_event_queue.peek().is_some_and(|evt|evt.time <= self.state.time) {
                // pop_if is nightly, so we use a less rusty alternative with unwrap()
                let evt = self.state.control_event_queue.pop().unwrap();
                match evt.event {
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
                self.worker.process_batch(self.state.ctx(&ctx));
            }
        }

    }
}
