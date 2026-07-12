pub struct WrappingIterU32 {
    i: u32,
    end: u32,
}

impl WrappingIterU32 {
    pub fn new(start: u32, end: u32) -> Self {
        Self {
            i: start,
            end,
        }
    }
}
impl std::iter::Iterator for WrappingIterU32 {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        let i = self.i;
        if i == self.end {
            return None
        }
        self.i = self.i.wrapping_add(1);
        return Some(i);
    }
}


// make it 32 if it needs to support a larger optical circuit
pub type OpHandle = u16;
pub type NodeId = u16;
pub type PortId = u8;
pub type SinkModeId = u8;
pub type WpSnowflake = u32;
pub type Time = u64;

#[derive(Clone)]
pub struct PortAddress {
    pub node: NodeId,
    pub port: PortId,
}

#[derive(Clone)]
pub struct SinkModeLocation {
    pub operator: OpHandle,
    pub mode: SinkModeId,
}

pub struct BatchConstraint {
    pub timeout: u64, // picoseconds
    pub max_size: usize,
}

pub struct BatchPolicy{
    pub period: u64,// picoseconds
    pub max_size: usize,
}

impl BatchPolicy {
    pub fn get_constraint(&self, start_time: Time) -> BatchConstraint {
        BatchConstraint {
            timeout: start_time + self.period,
            max_size: self.max_size,
        }
    }
}
