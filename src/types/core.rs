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
    pub fn next(&mut self) -> Option<u32> {
        let i = self.i;
        if i == self.end {
            return None
        }
        self.i = self.i.wrapping_add(1);
        return Some(i);
    }
}
