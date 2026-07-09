use crate::types::core::WrappingIterU32;


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
    start_index: u32,
    end_index: u32,
}

impl CellStateRegistry {
    pub fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two() && capacity % 32 == 0, "State Registry capacity should always be a power of two greater than 32");
        Self {
            buff: vec![0; capacity/32],
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
    pub fn len(&self) -> u32 {
        self.end_index.wrapping_sub(self.start_index)
    }
    pub fn capacity(&self) -> u32 {
        (self.buff.len() * 32) as u32
    }
    // these two shall remain readonly
    pub fn start_index(&self) -> u32 {
        return self.start_index;
    }
    pub fn end_index(&self) -> u32 {
        return self.end_index;
    }

    pub fn handles_from_front(&self) -> WrappingIterU32 {
        return WrappingIterU32::new(self.start_index, self.end_index);
    }
    pub fn resized(&self, new_capacity: usize) -> Self {
        let mut grown = Self::with_capacity(new_capacity);
        for i in self.handles_from_front() {
            grown.set(i, self.get(i));
        }
        grown.start_index = self.start_index;
        grown.end_index = self.end_index;
        grown
    }
    pub fn push_back(&mut self, state: CellState){
        self.set(self.end_index, state);
        self.end_index = self.end_index.wrapping_add(1);
    }
    pub fn drop_front(&mut self){
        self.start_index = self.start_index.wrapping_add(1);
    }
}



