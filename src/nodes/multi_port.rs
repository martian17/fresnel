// generalized n port setup. handles photonic cluster of n channels, so the maximum degree of
// interaction is n. Not using this as a blanket implementation as double pointer indirection in the
// 2d array causes a heavy memory penalty

use std::sync::mpsc::{sync_channel, Receiver, SyncSender, SendError, RecvError};
use std::thread;



// this is order 2, so only useful inside a 2 port component
// in the future we need a generalization for n port interactions
struct WaveCluster{
    // have to think of ways to make this more compact and localized
    packets: Vec<WavePacket>,
    start_time: u64,
    end_time: u64,
    // pairs of interacting packets
    interactions: Vec<(u32, u32)>,
}

struct SecondOrderWaveCluster {
}

impl NodeWorker {
    fn spawn(port_cnt: usize) -> Node {
        let mut tx_ports: Vec<TxPort> = Vec::new();
        let mut rx_ports: Vec<RxPort> = Vec::new();
        for _ in 0..port_cnt {
            let (tx, rx) = sync_channel::<PortMessage>(3);
            tx_ports.push(TxPort {
                time: 0,
                tx,
            });
            rx_ports.push(RxPort {
                time: 0,
                rx,
                iterator: Vec::new().into_iter().peekable(),
            });
            
        }
        let mut worker = NodeWorker {
            ports: rx_ports,
        };
        thread::spawn(move ||{
            worker.run()
        });
        Node {
            ports: tx_ports,
        }
    }
    pub fn get_batch(&mut self) -> WavePacket {
        // spouts out a self contained unit of wave pacekts
    }
    pub fn get_youngest_packet(&mut self) -> WavePacket {
        for i in self.ports() {
            
        }
    }
    pub fn youngest_port(&mut self) -> (usize, &mut RxPort) {
        self.ports
            .iter_mut()
            .enumerate()
            .min_by_key(|(_, port)| port.time)
            .unwrap()
    }
    fn run(&mut self) {

        loop {
            let (index, port) = self.youngest_port();
            let message = port.recv().unwrap();
        }
    }
}


fn main() {
    let node_a = NodeWorker::spawn(2);
}



