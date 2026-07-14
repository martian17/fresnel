// Written by claude code.
// Runs provably faster than std::collections::HashSet<u32>, on a smaller memory footprint. It also
// beats Vec<u32>::contains() even with just 32 elements (single cache line) and vec search with
// SIMD optimization, and it only consumes marginally more memory, at the worst case occupying
// around 37.5% of the allocated memory with amortized usage of 56.25, vs vector's worst case at 50% and average case of 75%. TLDR; it's only 25% worse, so it's worth it.
// yanked from the original repo with performance benchmark


// ============================================================================
// 2. U32OpenAddressSet — open addressing, linear probe (delta = 1),
//    hash(n) = (n << shift) >> shift, cache-line-aligned storage
// ============================================================================

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};

/// 128 covers both x86 (64 B lines) and Apple/ARM big cores (128 B lines).
const CACHE_LINE: usize = 128;

/// Sentinel for an empty slot. u32::MAX as a *key* is handled by `has_max`.
const EMPTY: u32 = u32::MAX;

/// 32 slots x 4 B = 128 B = one Apple line. Also keeps shift <= 27.
const MIN_SLOTS: usize = 32;

pub struct U32OpenAddressSet {
    slots: *mut u32,
    cap: usize,    // slot count, always a power of two
    shift: u32,    // 32 - log2(cap); hash(n) = (n << shift) >> shift
    len: usize,    // occupied slots (not counting the MAX flag)
    has_max: bool, // whether u32::MAX itself is a member
}

unsafe impl Send for U32OpenAddressSet {}
unsafe impl Sync for U32OpenAddressSet {}

impl U32OpenAddressSet {
    /// `capacity` = expected element count; slots sized for load <= 0.75.
    pub fn new(capacity: usize) -> Self {
        let slots_needed = capacity
            .saturating_mul(4)
            .div_ceil(3)
            .next_power_of_two()
            .max(MIN_SLOTS);
        Self::with_slots(slots_needed)
    }

    fn with_slots(cap: usize) -> Self {
        debug_assert!(cap.is_power_of_two() && cap >= MIN_SLOTS);
        let layout = Layout::from_size_align(cap * 4, CACHE_LINE).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut u32;
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        unsafe { std::ptr::write_bytes(ptr as *mut u8, 0xFF, cap * 4) };
        Self {
            slots: ptr,
            cap,
            shift: 32 - cap.trailing_zeros(),
            len: 0,
            has_max: false,
        }
    }

    /// Literal truncation hash: keep the low log2(cap) bits.
    #[inline(always)]
    fn hash(&self, n: u32) -> usize {
        ((n << self.shift) >> self.shift) as usize
    }

    #[inline]
    pub fn contains(&self, x: u32) -> bool {
        if x == EMPTY {
            return self.has_max;
        }
        let mask = self.cap - 1;
        let mut i = self.hash(x);
        unsafe {
            loop {
                let s = *self.slots.add(i);
                if s == x {
                    return true;
                }
                if s == EMPTY {
                    return false;
                }
                i = (i + 1) & mask;
            }
        }
    }

    /// Returns `true` if the value was inserted (i.e. was absent).
    #[inline]
    pub fn insert(&mut self, x: u32) -> bool {
        if x == EMPTY {
            let was = self.has_max;
            self.has_max = true;
            return !was;
        }
        if (self.len + 1) * 4 > self.cap * 3 {
            self.grow();
        }
        let mask = self.cap - 1;
        let mut i = self.hash(x);
        unsafe {
            loop {
                let s = *self.slots.add(i);
                if s == x {
                    return false;
                }
                if s == EMPTY {
                    *self.slots.add(i) = x;
                    self.len += 1;
                    return true;
                }
                i = (i + 1) & mask;
            }
        }
    }

    /// Backward-shift deletion (no tombstones).
    pub fn remove(&mut self, x: u32) -> bool {
        if x == EMPTY {
            let was = self.has_max;
            self.has_max = false;
            return was;
        }
        let mask = self.cap - 1;
        let mut i = self.hash(x);
        unsafe {
            loop {
                let s = *self.slots.add(i);
                if s == EMPTY {
                    return false;
                }
                if s == x {
                    break;
                }
                i = (i + 1) & mask;
            }
            let mut hole = i;
            let mut j = (i + 1) & mask;
            loop {
                let s = *self.slots.add(j);
                if s == EMPTY {
                    break;
                }
                let home = self.hash(s);
                let dist_home_to_j = j.wrapping_sub(home) & mask;
                let dist_hole_to_j = j.wrapping_sub(hole) & mask;
                if dist_home_to_j >= dist_hole_to_j {
                    *self.slots.add(hole) = s;
                    hole = j;
                }
                j = (j + 1) & mask;
            }
            *self.slots.add(hole) = EMPTY;
            self.len -= 1;
            true
        }
    }

    fn grow(&mut self) {
        let old_cap = self.cap;
        let old_self = std::mem::replace(self, Self::with_slots(old_cap * 2));
        self.has_max = old_self.has_max;
        unsafe {
            for i in 0..old_cap {
                let s = *old_self.slots.add(i);
                if s != EMPTY {
                    let mask = self.cap - 1;
                    let mut j = self.hash(s);
                    while *self.slots.add(j) != EMPTY {
                        j = (j + 1) & mask;
                    }
                    *self.slots.add(j) = s;
                    self.len += 1;
                }
            }
        }
    }

    /// Reset to empty without deallocating (memset of cap*4 bytes).
    #[inline]
    pub fn clear(&mut self) {
        unsafe { std::ptr::write_bytes(self.slots as *mut u8, 0xFF, self.cap * 4) };
        self.len = 0;
        self.has_max = false;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len + self.has_max as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slot_capacity(&self) -> usize {
        self.cap
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        let slots = self.slots;
        let body = (0..self.cap).filter_map(move |i| {
            let s = unsafe { *slots.add(i) };
            (s != EMPTY).then_some(s)
        });
        body.chain(self.has_max.then_some(u32::MAX))
    }
}

impl Drop for U32OpenAddressSet {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.cap * 4, CACHE_LINE).unwrap();
        unsafe { dealloc(self.slots as *mut u8, layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_addr_basic_growth_and_max() {
        let mut s = U32OpenAddressSet::new(0);
        assert_eq!(s.slot_capacity(), 32);
        for i in 0..1000u32 {
            assert!(s.insert(i * 3));
        }
        for i in 0..1000u32 {
            assert!(s.contains(i * 3));
            assert!(!s.contains(i * 3 + 1));
        }
        assert!(s.insert(u32::MAX));
        assert!(s.contains(u32::MAX));
        assert_eq!(s.len(), 1001);
        s.clear();
        assert!(s.is_empty());
        assert!(!s.contains(u32::MAX));
    }

    #[test]
    fn open_addr_cluster_removal() {
        let mut s = U32OpenAddressSet::new(16);
        let keys: Vec<u32> = (0..12u32).map(|i| (i << 20) | 5).collect();
        for &k in &keys {
            assert!(s.insert(k));
        }
        assert!(s.remove(keys[5]));
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(s.contains(k), i != 5);
        }
    }
}
