use std::sync::mpsc::{Receiver, SyncSender, SendError, RecvError};

pub struct WavePacket {
    pub time: u64,// ps
    pub time_spread: u32,// ps, three sigma
    pub wl: f32,
    pub wl_spread: f32,
    pub qs_handle: u32,
    pub snowflake: u32,
}

impl WavePacket {
    pub fn start_time(&self) -> u64 {
        // three sigma
        return self.time - self.time_spread as u64 * 3;
    }
    pub fn end_time(&self) -> u64 {
        return self.time + self.time_spread as u64 * 3;
    }
    pub fn overlaps(&self, wp: &WavePacket) -> bool {
        return self.start_time() < wp.end_time() && wp.start_time() < self.end_time();
    }
    pub fn is_strictly_before(&self, reference_time: u64) -> bool {
        return self.end_time() < reference_time;
    }
}

pub struct WPBatch {
    pub start_time: u64,
    pub end_time: u64,
    pub batch: Vec<WavePacket>,
}

pub enum PortMessage {
    Batch(WPBatch),
    // Close,// close port
}

pub struct TxPort {
    pub time: u64,
    pub tx: SyncSender<PortMessage>,
}

impl TxPort {
    pub fn send_batch(&mut self, message: WPBatch) -> Result<(), SendError<PortMessage>> {
        self.time = message.end_time;
        self.tx.send(PortMessage::Batch(message))
    }
}

pub struct RxPort{
    pub time: u64,
    pub rx: Receiver<PortMessage>,
    pub iterator: std::iter::Peekable<std::vec::IntoIter<WavePacket>>,
    // now it owns the wave packets inside the iterator, but in case it shouldn't I will still keep
    // the borrowed representaton. If we choose to use the borrowed version we need to add a
    // lifetime on the parent struct as well
    // iterator: std::slice::Iter<'a, WavePacket>
    // owned_packets: Vec<WavePacket>,
    
}

pub enum RxTick {
    Wp(WavePacket),
    Time(u64),
}

impl RxTick {
    pub fn time(&self) -> u64 {
        match self {
            Self::Wp(wp) => wp.start_time(),
            Self::Time(time) => *time,
        }
    }
}

impl RxPort{
    fn recv(&mut self) -> Result<PortMessage, RecvError> {
        match self.rx.recv() {
            Ok(message) => {
                self.time = match &message {
                    PortMessage::Batch(batch) => batch.start_time,
                };
                Ok(message)
            },
            Err(e) => Err(e),
        }
    }
    pub fn get_until(&mut self, time: u64) -> Vec<WavePacket> {
        let mut res: Vec<WavePacket> = Vec::new();
        loop {
            if let Some(wp) = self.iterator.peek() {
                if wp.time < time {
                    let wp = self.iterator.next().unwrap();
                    self.time = wp.time;
                    res.push(wp);
                } else {
                    break;
                }
            } else {
                let msg = self.recv().unwrap();
                let time = match msg {
                    PortMessage::Batch(batch) => batch.start_time,
                };
            }
        }
        res
    }
    pub fn next_packet(&mut self, time_limit: u64) -> WavePacket {
        loop {
            if let Some(wp) = self.iterator.next() {
                return wp;
            }
            println!("Warning: Unckecked RxPort::next_packet call");
            println!("This might happen if the node stays disconnected after the creation process");
            self.iterator = match self.recv().unwrap() {
                PortMessage::Batch(batch) => batch.batch.into_iter().peekable(),
            };
            continue;
        }
    }
    pub fn tick(&mut self) -> RxTick {
        if let Some(wp) = self.iterator.next() {
            return RxTick::Wp(wp);
        } else {
            let msg = self.recv().unwrap();
            let batch = match msg {
                PortMessage::Batch(batch) => batch,
            };
            self.iterator = batch.batch.into_iter().peekable();
            return RxTick::Time(batch.start_time);
        }
    }
    pub fn peek_next_packet(&mut self) -> Option<&WavePacket> {
        self.iterator.peek()
    }
    // for dual port configuration
    pub fn check_overlap(&mut self, ref_packet: &WavePacket) -> bool {
        loop {
            if let Some(wp) = self.iterator.peek() {
                return  wp.overlaps(ref_packet);
            }
            // it is None, iterator has ran out of wave packets
            if ref_packet.is_strictly_before(self.time) {
                // check the channel local time first
                return false;
            } else {
                self.iterator = match self.recv().unwrap() {
                    PortMessage::Batch(batch) => batch.batch.into_iter().peekable(),
                };
                continue;
            }
        }
    }
}


pub struct WorkerHandle {
    pub ports: Vec<TxPort>,
}
