use std::sync::mpsc::{sync_channel, channel, Receiver, SyncSender, SendError, RecvError};
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;

use crate::nodes::core::{
    WavePacket,
    WpBatch,
    TxPort,
    RxPort,
    WorkerHandle,
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
};




// models 
struct SinglePortWorker {

    port: RxPort,
    batch_period: u64,
    batch_size: usize,
    id: u16,
    time: Time,
    store: Arc<InteractionStore>,
    sink: Option<(PortAddress, TxPort)>,
    control_channel: Receiver<ControlEvent>,
    control_event_queue: BinaryHeap<ControlEvent>,
    // jones matrix
    // but extended as kraus operators

    // max u32 picosecond time corresponds to 4ms, which is about 1200km in vacuum distance
    // which is still not out of the realm of possibility, especially with satellite based
    // communication, so we still use u64 here
    time_delay: u64,
}



impl SinglePortWorker {
    pub fn spawn(store: Arc<InteractionStore>, id: u16, time_delay: u64) -> WorkerHandle {
        let (tx_raw, rx_raw) = sync_channel::<WpBatch>(3);
        let (control_tx, control_rx) = channel::<ControlEvent>();
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
        let mut worker = Self {
            port: rx,
            // 20 us gives us approx. 200 wave packets
            // hardcode this as picoseconds for now
            // this represents about 6.4kb of memory vs L1 cache which is
            // generally around 32 to 100kb
            batch_period: 20_000_000,
            batch_size: 200,
            id,
            time: 0,
            store,
            sink: None,
            control_channel: control_rx,
            control_event_queue: BinaryHeap::new(),
            time_delay,
        };
        thread::spawn(move ||{
            worker.run();
        });
        WorkerHandle {
            ports: vec![tx],
            control: control_tx,
        }
    }
    fn handle_control_event(&mut self) {
        while let Ok(evt) = self.control_channel.try_recv() {
            self.control_event_queue.push(evt);
        }
        while self.control_event_queue.peek().is_some_and(|evt|evt.time <= self.time) {
            // pop_if is nightly, so we use a less rusty alternative with unwrap()
            let evt = self.control_event_queue.pop().unwrap();
            match evt.event_type {
                ControlEventType::ConnectSink {sink_tx, address} => {
                    self.sink = Some((address, sink_tx));
                }
            }
        }
    }
    fn run(&mut self) {
        loop {
            self.handle_control_event();
            // since it's a single port, no need to worry about boundary condition
            // therefore we just get a batch here
            let mut batch = self.port.get_batch(BatchConstraint{
                timeout: self.port.current_time + self.batch_period,
                max_size: self.batch_size,
            });
            let slice = self.store.get_states(vec![&mut batch.batch]);
            let sink_batch: Vec<WavePacket> = Vec::new();
            for wp in batch.batch {
                let state = match slice.get_mut(wp.state_handle) {
                    InteractionCell::IslandOfInteraction(state) => state,
                    _cell => {
                        // TODO: Make this error nicer
                        panic!("Expected IslandOfInteraction, but got something else"); 
                    }
                };
                let sink_mode = state.active_packets.extract(wp.snowflake);
                let op_handle = state.add_operator(Operator::Single{
                    node: self.id,
                    time: wp.time,
                    // these are placeholders
                    sink: (0, 0),
                });
                state.set_sink(sink_mode, op_handle);
            }

            if let Some((address, port)) = &mut self.sink {
                port.send_batch(WpBatch{
                    start_time: batch.start_time + self.time_delay,
                    end_time: batch.end_time + self.time_delay,
                    batch: sink_batch,
                });
            } else {
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
}


