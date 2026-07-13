// Written with the help of ChatGPT
// https://chatgpt.com/c/6a20ae3c-b2e4-83a8-a458-965a786cc85d
//
// # Prompts
// * should I create my own snowflake factory in rust?
// * I need a very high rate of generation. Targeting a billion across different cores
// * * Definitely thread local
//   * Opaque ID assignment through mutex (the user just needs to call a function)
//   * 1 byte for process ID, the rest is incremented. It's okay if it loops back to 0 in my use case, since each object is short lived
//   * Implement two version; 32 bit and 64 bit. Both of them use 8 bits for core ID, and the rest is incremented and looped to 0 when it overflows
// * Write snowflake.rs


use std::cell::Cell;
use std::sync::{Mutex, OnceLock};

struct IdAllocator {
    next: u8,
}

impl IdAllocator {
    fn allocate(&mut self) -> u8 {
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        id
    }
}

fn allocate_generator_id() -> u8 {
    static ALLOCATOR: OnceLock<Mutex<IdAllocator>> = OnceLock::new();

    let allocator = ALLOCATOR.get_or_init(|| {
        Mutex::new(IdAllocator { next: 0 })
    });

    allocator.lock().unwrap().allocate()
}

struct Generator32 {
    id: u8,
    counter: Cell<u32>,
}

impl Generator32 {
    fn new() -> Self {
        Self {
            id: allocate_generator_id(),
            counter: Cell::new(0),
        }
    }

    #[inline(always)]
    fn next(&self) -> u32 {
        let counter = self.counter.get().wrapping_add(1);
        self.counter.set(counter);

        ((counter & 0x00FF_FFFF) << 8) | self.id as u32
    }
}

struct Generator64 {
    id: u8,
    counter: Cell<u64>,
}

impl Generator64 {
    fn new() -> Self {
        Self {
            id: allocate_generator_id(),
            counter: Cell::new(0),
        }
    }

    #[inline(always)]
    fn next(&self) -> u64 {
        let counter = self.counter.get().wrapping_add(1);
        self.counter.set(counter);

        ((counter & 0x00FF_FFFF_FFFF_FFFF) << 8) | self.id as u64
    }
}

thread_local! {
    static GEN32: Generator32 = Generator32::new();
    static GEN64: Generator64 = Generator64::new();
}

#[inline(always)]
pub fn next_u32() -> u32 {
    GEN32.with(Generator32::next)
}

#[inline(always)]
pub fn next_u64() -> u64 {
    GEN64.with(Generator64::next)
}

pub type Snowflake = u64;
