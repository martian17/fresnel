use std::sync::mpsc::{sync_channel, Receiver, SyncSender, SendError, RecvError};
use std::thread;

use crate::nodes::core::{
    WavePacket,
    PortMessage,
    TxPort,
    RxPort,
    WorkerHandle,
    RxTick,
};


// models 
struct SinglePortWorker {
    port: RxPort,
    batch_period: u64,
    // jones matrix
    // but extended as kraus operators
}


impl SinglePortWorker {
    fn spawn() -> WorkerHandle {
        let (tx_raw, rx_raw) = sync_channel::<PortMessage>(3);
        let tx = TxPort {
            time: 0,
            tx: tx_raw,
        };
        let rx = RxPort {
            time: 0,
            rx: rx_raw,
            // set the empty iterator
            iterator: Vec::new().into_iter().peekable(),
        };
        let mut worker = Self {
            port: rx,
            // 100 us gives us approx. 1000 wave packets
            // hardcode this as picoseconds for now
            // we might want to use a more sophisticated batching heuristics
            batch_period: 100_000_000,
        };
        thread::spawn(move ||{
            worker.run();
        });
        WorkerHandle {
            ports: vec![tx],
        }
    }
    fn run(&mut self, ) {
        loop {
            let batch_start = self.port.time;
            let mut packets: Vec<WavePacket> = Vec::new();
            loop {
                let tick = self.port.tick();
                let tick_time = tick.time();
                if let RxTick::Wp(wp) = tick {
                    packets.push(wp);
                }
                if tick_time - batch_start >= self.batch_period {
                    break;
                }
            }
            
        }
    }
}


