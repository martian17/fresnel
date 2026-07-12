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

struct WorkerContext {
    interaction_store: InteractionStore,
}

struct NodeRunner<T: NodeWorker> {
    rx_ports: Vec<RxPort>,
    control_rx: Receiver<TimedControlEvent<T::CustomControlEvent>>,
    control_event_queue: BinaryHeap<TimedControlEvent<T::CustomControlEvent>>,
    time: Time,
    worker: T,
}



trait NodeWorker: Send {
    type CustomControlEvent: Send;
    type NodeTemplate;
    type NodeHandle: NodeHandle<CustomControlEvent = Self::CustomControlEvent, NodeTemplate = Self::NodeTemplate>;

    fn new(template: &Self::NodeTemplate) -> Self;
    fn handle_connection(&mut self, ctx: RunnerContext<Self::CustomControlEvent>, exit_port_id: PortId, tx_port: TxPort);
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self::CustomControlEvent>, custom_event: Self::CustomControlEvent);
    fn process_batch(&mut self, ctx: RunnerContext<Self::CustomControlEvent>);

}

struct RunnerState<'a, CustomControlEvent> {
    rx_ports: &'a mut Vec<RxPort>,
    control_rx: &'a mut Receiver<TimedControlEvent<CustomControlEvent>>,
    control_event_queue: &'a mut BinaryHeap<TimedControlEvent<CustomControlEvent>>,
    time: &'a mut Time,
}

struct RunnerContext<'a, CustomControlEvent>{
    runner: RunnerState<'a, CustomControlEvent>,
    global: Arc<WorkerContext>,
}


impl<T: NodeWorker + 'static> NodeRunner<T> {
    fn spawn(ctx: Arc<WorkerContext>, rx_port_count: usize, template: T::NodeTemplate) -> T::NodeHandle {
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
            rx_ports,
            control_rx,
            control_event_queue: BinaryHeap::new(),
            time: 0,
            worker: T::new(&template),
        };
        let handle = thread::spawn(move || {
            runner.run(ctx);
        });
        T::NodeHandle::new(&template, handle)
    }
    fn preload_control_events(&mut self){
        loop {
            let evt = self.control_rx.recv().unwrap();

            match evt.event {
                NodeControlEvent::Start => {
                    self.time = evt.time;
                },
                _ => {
                    self.control_event_queue.push(evt);
                }
            }
        }
    }
    fn run(&mut self, ctx: Arc<WorkerContext>){
        self.preload_control_events();
        loop {
            while let Ok(evt) = self.control_rx.try_recv() {
                self.control_event_queue.push(evt);
            }
            while self.control_event_queue.peek().is_some_and(|evt|evt.time <= self.time) {
                // pop_if is nightly, so we use a less rusty alternative with unwrap()
                let evt = self.control_event_queue.pop().unwrap();
                let runner_context = RunnerContext {
                    runner: RunnerState{
                        rx_ports: &mut self.rx_ports,
                        control_rx: &mut self.control_rx,
                        control_event_queue: &mut self.control_event_queue,
                        time: &mut self.time,
                    },
                    global: ctx.clone(),
                };
                match evt.event {
                    NodeControlEvent::Start => {
                        panic!("double start is currently not supported");
                    },
                    NodeControlEvent::Stop => {
                        // let the thread join
                        return;
                    },
                    NodeControlEvent::Connect{exit_port_id, tx_port} => {
                        self.worker.handle_connection(runner_context, exit_port_id, tx_port);
                    },
                    NodeControlEvent::Custom(custom_event) => {
                        self.worker.handle_custom_event(runner_context, custom_event);
                    }
                }
                let runner_context = RunnerContext {
                    runner: RunnerState{
                        rx_ports: &mut self.rx_ports,
                        control_rx: &mut self.control_rx,
                        control_event_queue: &mut self.control_event_queue,
                        time: &mut self.time,
                    },
                    global: ctx.clone(),
                };
                self.worker.process_batch(runner_context);
            }
        }

    }
}



// trait NodeWorker {
//     type CustomControlEvent;
//     type WorkerTemplate;
// 
//     // user defined methods
//     fn build(ctx: Arc<WorkerContext>, template: Self::WorkerTemplate) -> NodeWorker;
//     fn build_handle(&self, join_handle: std::thread::JoinHandle<()>) -> NodeHandle;
//     fn handle_connection(&mut self, exit_port_id: PortId, tx_port: TxPort);
//     fn handle_custom_event(&mut self, custom_event: Self::CustomControlEvent);
//     // mandatory iterm getters/setters
//     fn get_rx_ports(&self) -> &Vec<RxPort>;
//     fn get_control_channel(&self) -> &Receiver<TimedControlEvent<Self::CustomControlEvent>>;
//     fn get_control_event_queue(&self) -> &mut BinaryHeap<TimedControlEvent<Self::CustomControlEvent>>;
//     fn set_time(&self, time: Time);
//     fn get_time(&self) -> Time;
// 
// 
// 
//     // derived methods
//     fn spawn(ctx: Arc<WorkerContext>, template: Self::WorkerTemplate) -> Self {
//         let worker = Self::build(ctx, template);
//         let join_handle = thread::spawn(move || {
//             worker.run();
//         });
//         worker.build_handle()
//     }
// 
//     fn run(&mut self){
//         self.preload_control_events();
//         loop {
//             while let Ok(evt) = self.get_control_channel().try_recv() {
//                 self.get_control_event_queue().push(evt);
//             }
//             while self.get_control_event_queue().peek().is_some_and(|evt|evt.time <= self.get_time()) {
//                 // pop_if is nightly, so we use a less rusty alternative with unwrap()
//                 let evt = self.get_control_event_queue().pop().unwrap();
//                 match evt.event {
//                     NodeControlEvent::Start => {
//                         panic!("double start is currently not supported");
//                     },
//                     NodeControlEvent::Stop => {
//                         // let the thread join
//                         return;
//                     },
//                     NodeControlEvent::Connect{exit_port_id, tx_port} => {
//                         self.handle_connection(exit_port_id, tx_port);
//                     },
//                     NodeControlEvent::Custom(custom_event) => {
//                         self.handle_custom_event(custom_event);
//                     }
//                 }
//                 self.process_batch();
//             }
//         }
// 
// 
//     }
//     
// 
//     fn preload_control_events(&mut self){
//         loop {
//             let evt = self.get_control_channel().recv().unwrap();
// 
//             match evt.event {
//                 NodeControlEvent::Start => {
//                     self.set_time(evt.time);
// 
//                 },
//                 _ => {
//                     self.get_control_event_queue().push(evt);
//                 }
//             }
//         }
//     }
// }
// 
// 
// 
// 
// enum SinglePortEvent {
//     SetDelay(u64),
// }
// 
// 
// struct SinglePortWorkerHandle {
//     pub ports: Vec<TxPort>,
//     pub control: Sender<ControlEvent>
// }
// 
// impl DefaultControl for SinglePortWorkerHandle {
//     fn get_ports(&self) -> &Vec<TxPort> {
//         self.ports
//     }
//     
//     fn get_control(&self) -> 
//     fn get_port(&self, port_id: PortId) -> TxPort {
//         self.ports
//     }
//     fn get_ports() {
// 
//     }
// }
// 
// 
