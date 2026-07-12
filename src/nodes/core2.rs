#![allow(unused_imports)]
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender, SendError, RecvError};
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

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


// This enum and the timed event are relatively stable

enum NodeControlEvent<CustomControlEvent> {
    Start,
    Stop,
    Connect {
        exit_port_id: PortId,
        tx_port: TxPort,
    },
    Custom(CustomControlEvent),
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

struct NodeHandleCore<CustomControlEvent>{
    tx_ports: Vec<TxPort>,
    control_channel: Sender<TimedControlEvent<CustomControlEvent>>,
    join_handle: std::thread::JoinHandle<()>,
}


struct EntryPortHandle{
    tx: TxPort,
}


struct ExitPortHandle<'a, CustomControlEvent> {
    node_handle: &'a NodeHandleCore<CustomControlEvent>,
    exit_port_id: PortId,
}

impl<'a, CustomControlEvent> ExitPortHandle<'a, CustomControlEvent> {
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


impl<CustomControlEvent> NodeHandleCore<CustomControlEvent>{
    fn entry_port(&self, id: PortId) -> EntryPortHandle {
        EntryPortHandle {
            tx: self.tx_ports[id],
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
