use crate::types::core::Time;
use std::cmp::Ordering;

pub struct Timed<T> {
    time: Time,
    value: T,//NodeControlEvent<T>
}

impl <T> Timed<T> {
    pub fn from(value: T, time: Time) -> Self {
        Timed{
            time,
            value,
        }
    }
}

impl <T>PartialEq for Timed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl <T>Eq for Timed<T> {}

impl <T>Ord for Timed<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse the standard comparison to create a min-heap
        other.time.cmp(&self.time)
    }
}

impl <T>PartialOrd for Timed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub trait HasCore<Core> {
    fn core_mut(&mut self) -> &mut Core;
    fn core(&self) -> &Core;
    fn into_core(self) -> Core;
}

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
