use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender, SendError, RecvError};
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;

use crate::nodes::core::{
    WavePacket,
    WpBatch,
    TxPort,
    Connection,
    RxPort,
    // WorkerHandle,
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


pub enum EPPSPort{
    Signal,
    Idler,
}



enum EPPSControlEventType {
    // Common events
    Start,
    Shutdown,

    // Node specific events
    Connect {
        port: EPPSPort,
        conn: Connection,
    },
    SetWaveProfile {
        port: EPPSPort,
        profile: WaveProfile,
    },
    SetPumpFrequency(f64),
}

type EPPSControlEvent = ControlEvent<EPPSControlEventType>;



struct EPPSWorkerHandle {
    // no optical reception ports, just controls
    pub control: Sender<EPPSControlEvent>,
}


// TODO: Shove some of these in a trait
impl EPPSWorkerHandle {
    fn start(&mut self, time: Time) {
        self.control.send(EPPSControlEvent{
            time,
            event_type: EPPSControlEventType::Start,
        });
    }
    fn shutdown(&mut self, time: Time) {
        self.control.send(EPPSControlEvent{
            time,
            event_type: EPPSControlEventType::Shutdown,
        });
    }

    fn connect_signal(&self, conn: Connection) {
        self.control.send(EPPSControlEvent{
            time: 0,
            event_type: EPPSControlEventType::Connect {
                port: EPPSPort::Signal,
                conn,
            }
        });
    }
    fn connect_idler(&self, conn: Connection) {
        self.control.send(EPPSControlEvent{
            time: 0,
            event_type: EPPSControlEventType::Connect {
                port: EPPSPort::Idler,
                conn,
            }
        });
    }

    fn set_signal_wave_profile(&self, profile: WaveProfile, time: Time) {
        self.control.send(EPPSControlEvent{
            time,
            event_type: EPPSControlEventType::SetWaveProfile {
                port: EPPSPort::Signal,
                profile,
            }
        });
    }
    fn set_idler_wave_profile(&self, profile: WaveProfile, time: Time) {
        self.control.send(EPPSControlEvent{
            time,
            event_type: EPPSControlEventType::SetWaveProfile {
                port: EPPSPort::Idler,
                profile,
            }
        });
    }

    fn set_pump_frequency(&self, frequency: f64, time: Time) {
        self.control.send(EPPSControlEvent{
            time,
            event_type: EPPSControlEventType::SetPumpFrequency(frequency),
        });
    }

}



struct WaveProfile{
    time_sigma: u32,
    wavelength: f32,
    wavelength_sigma: f32,
}

struct EPPSTemplate {
    signal_profile: WaveProfile,
    idler_profile: WaveProfile,
    pump_frequency: f64,
}


// models 
struct EPPSWorker {
    // properties needed by every node worker
    batch_period: u64,
    batch_size: usize,
    id: u16,
    time: Time,
    store: Arc<InteractionStore>,
    control_channel: Receiver<EPPSControlEvent>,
    control_event_queue: BinaryHeap<EPPSControlEvent>,

    // shape specific
    signal_sink: Option<(PortAddress, TxPort)>,
    idler_sink: Option<(PortAddress, TxPort)>,

    signal_profile: WaveProfile,
    idler_profile: WaveProfile,

    pump_frequency: f64,
}


impl EPPSWorker {
    pub fn spawn(
        store: Arc<InteractionStore>,
        id: u16,
        template: EPPSTemplate,
    ) -> EPPSWorkerHandle {
        let (control_tx, control_rx) = channel::<EPPSControlEvent>();
        let mut worker = Self {
            batch_period: 20_000_000,
            batch_size: 200,
            id,
            time: 0,
            store,
            control_channel: control_rx,
            control_event_queue: BinaryHeap::new(),

            signal_sink: None,
            idler_sink: None,

            signal_profile: template.signal_profile,
            idler_profile: template.idler_profile,

            pump_frequency: template.pump_frequency,
        };
        thread::spawn(move ||{
            worker.run();
        });
        EPPSWorkerHandle {
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


