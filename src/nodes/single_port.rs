use std::sync::mpsc::{sync_channel, Receiver, SyncSender, SendError, RecvError};
use std::thread;

use crate::nodes::core::{
    WavePacket,
    WpBatch,
    TxPort,
    RxPort,
    WorkerHandle,
    BatchConstraint,
};


// models 
struct SinglePortWorker {
    port: RxPort,
    batch_period: u64,
    batch_size: usize,
    // jones matrix
    // but extended as kraus operators
}


impl SinglePortWorker {
    fn spawn() -> WorkerHandle {
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
        let mut worker = Self {
            port: rx,
            // 20 us gives us approx. 200 wave packets
            // hardcode this as picoseconds for now
            // this represents about 6.4kb of memory vs L1 cache which is
            // generally around 32 to 100kb
            batch_period: 20_000_000,
            batch_size: 200,
        };
        thread::spawn(move ||{
            worker.run();
        });
        WorkerHandle {
            ports: vec![tx],
        }
    }
    fn run(&mut self) {
        loop {
            // since it's a single port, no need to worry about boundary condition
            // therefore we just get a batch here
            let batch = self.port.get_batch(BatchConstraint{
                timeout: self.port.current_time + self.batch_period,
                max_size: self.batch_size,
            });
            let batches = vec![&mut batch];

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


