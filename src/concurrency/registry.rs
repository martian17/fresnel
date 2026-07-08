#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum CellState {
    Free,
    Locked,
    Moved,
    Retired,
}

impl CellState {
    fn from_bits(n: u64) -> CellState {
        // layout
        // 0b0000...00xx
        let bits = n & 0b11;
        match bits {
            0 => CellState::Free,
            1 => CellState::Locked,
            2 => CellState::Moved,
            3 => CellState::Retired,
            _ => unreachable!("This is alredy capped to 3 by the bitmap"),
        }
    }
    fn to_bits(&self) -> u64 {
        match self {
            CellState::Free => 0,
            CellState::Locked => 1,
            CellState::Moved => 2,
            CellState::Retired => 3,
        }
    }
}

pub struct CellStateRegistry {
    buff: Vec<u64>,
    pub start_index: u32,
    pub end_index: u32,
}

impl CellStateRegistry {
    pub fn new() -> Self {
        Self {
            buff: vec![0; 512],
            start_index: 0,
            end_index: 0,
        }
    }
    pub fn get(&self, i: u32) -> CellState {
        let idx = ((i / 32) as usize) % self.buff.len();
        let offset = (i % 32) << 1;
        let n = self.buff[idx];
        CellState::from_bits(n>>offset)
    }
    pub fn set(&mut self, i: u32, state: CellState){
        let idx = ((i / 32) as usize) % self.buff.len();
        let offset = (i % 32) << 1;
        let mut n = self.buff[idx];
        n &= !(0b11 << offset);
        n |= state.to_bits() << offset;
        self.buff[idx] = n;
    }
    pub fn push_back(&mut self, state: CellState){
        let len = self.end_index.wrapping_sub(self.start_index) as usize;
        let capacity = self.buff.len() * 32;
        if len < capacity {
            self.set(self.end_index, state);
            self.end_index = self.end_index.wrapping_add(1);
        } else {
            let buff_len = self.buff.len();
            // might want to use unsafe alloc in the future, if this becomes bottleneck, though unlikely
            self.buff.resize(buff_len * 2, 0);
            let old_head_idx = (self.start_index / 32) as usize % buff_len;
            let old_tail_idx = (self.end_index / 32) as usize % buff_len;
            let new_head_idx = (self.start_index / 32) as usize % (buff_len * 2);
            // new_tail_idx ended up not being used in the commparison, but leaving it here just for
            // the sake of completeness.
            // let new_tail_idx = (self.end_index / 32) as usize % (buff_len * 2);
            if old_head_idx == new_head_idx {
                // tail got unwrapped
                // +1 just to be safe. doesn't matter if junk gets copied. the range is captured by
                // start_index and end_index anyways.
                for i in 0..(old_tail_idx+1).min(buff_len) {
                    self.buff[buff_len + i] = self.buff[i];
                }
            } else {
                // head got shifted
                for i in old_head_idx..buff_len {
                    self.buff[buff_len + i] = self.buff[i];
                }
            }
            self.set(self.end_index, state);
            self.end_index = self.end_index.wrapping_add(1);
        }
    }
    pub fn drop_front(&mut self) {
        self.start_index = self.start_index.wrapping_add(1);
    }
    pub fn len(&self) -> u32 {
        self.end_index.wrapping_sub(self.start_index)
    }
}



