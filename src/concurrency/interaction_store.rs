use std::sync::{Mutex, Condvar, Arc};
// use std::rc::{Rc};
use std::cell::UnsafeCell;
use smallvec::{SmallVec, smallvec};

use crate::concurrency::registry::{
    CellState,
    CellStateRegistry,
};


// make it 32 if it needs to support a larger optical circuit
type OpIndex = u16;
type NodeId = u16;
type PortId = u8;


#[derive(Clone)]
struct EPPSEntity {
    node: NodeId,
    time: u64,
    // todo: these could be possibly compacted to u32
    left: (OpIndex, PortId),
    right: (OpIndex, PortId),
}

#[derive(Clone)]
enum Operator {
    Single {
        node: NodeId,
        time: u64,
        sink: (OpIndex, PortId),
    },
    // These represent 2x2 linear compoents
    // The worker thread then queries the actual Kraus oOperator and Scatter Matrix using
    // (NodeId, time)
    //
    // 2x2 component with interference. Photon incidence on both ports
    DualBivariate {
        node: NodeId,
        time: u64,
        // superficial similarties of the packets
        // teporal and frequential overlap (unit inner product)
        // max is 1.0
        packet_similarity: f64,
        sink_left: (OpIndex, OpIndex, PortId),
        sink_right: (OpIndex, OpIndex, PortId),
    },
    // 2x2 component without interference. One port at a time
    DualUnivariate {
        node: NodeId,
        time: u64,
        incidence_port_id: PortId,
        sink_left: (OpIndex, PortId),
        sink_right: (OpIndex, PortId),
    },
    SPD {
        node: NodeId,
        time: u64,
    }
}


#[derive(Clone)]
struct IslandOfInteraction {
    operators: SmallVec<[Operator; 13]>,
    eppss: SmallVec<[EPPSEntity; 2]>,
}

#[derive(Clone)]
struct Tombstone{
    // 256 qubits is more than too much to handle already
    // so u8 suffices
    ref_cnt: u8,
    move_idx: u32,
}


// this needs to be aligned to \pmod 128
#[derive(Clone)]
#[repr(align(128))]
pub enum InteractionCell {
    None,
    Tombstone(Tombstone),
    IslandOfInteraction(IslandOfInteraction),
}


pub struct InteractionStore {
    // TODO: Replace this with RwLock and benchmark
    // RwLock might be slower because of true sharing
    registry: Mutex<CellStateRegistry>,
    buff: Arc<[UnsafeCell<InteractionCell>]>,

    // virtual index inside the u32 space
    start_index: u32,
    end_index: u32,
    cvar: Condvar,
}

pub struct InteractionStoreSlice<'a> {
    parent: &'a InteractionStore,
    // borrowed buffer from parent
    //buff: &'a mut 
    buff: Arc<[UnsafeCell<InteractionCell>]>,
    indices: Vec<u32>,
}

impl<'a> Drop for InteractionStoreSlice<'a> {
    fn drop(&mut self) {
        let mut registry = self.parent.registry.lock().unwrap();
        // if the buffer got realloced, move the results over
        if !Arc::ptr_eq(&self.parent.buff, &self.buff) {
            let old_buff = &self.buff;
            let new_buff = &self.parent.buff;
            let old_len = old_buff.len();
            let new_len = new_buff.len();
            for i in self.indices.iter().copied() {
                let old_idx = i as usize % old_len;
                let new_idx = i as usize % new_len;
                copy_unsafecell(&old_buff[old_idx], &new_buff[new_idx]);
            }
        }
        for i in self.indices.iter().copied() {
            registry.unlock(i);
        }
        self.parent.cvar.notify_all();
    }
}





// unsafe fn get_mut<T>(ptr: &UnsafeCell<T>) -> &mut T {
//   unsafe { &mut *ptr.get() }
// }
// 
// fn get_shared<T>(ptr: &mut T) -> &UnsafeCell<T> {
//   let t = ptr as *mut T as *const UnsafeCell<T>;
//   // SAFETY: `T` and `UnsafeCell<T>` have the same memory layout
//   unsafe { &*t }
// }

fn new_statecell_buffer(size: usize) -> Arc<[UnsafeCell<InteractionCell>]>{
    (0..size)
        .map(|_| UnsafeCell::new(InteractionCell::None))
        .collect::<Vec<_>>()
        .into()
}

fn copy_unsafecell<T>(src: &UnsafeCell<T>, dst: &UnsafeCell<T>) {
    unsafe {
        // according to Gemini, direct memory copy lets you copy things over without
        // unwanted .drop() callings
        core::ptr::copy_nonoverlapping(src.get(), dst.get(), 1);
    }
}

fn set_unsafecell<T>(dst: &UnsafeCell<T>, val: T) {
    unsafe {
        core::ptr::write(dst.get(), val);
    }
}

impl InteractionStore {
    fn new() -> Self {
        let init_buff_size = 512;
        Self {
            registry: Mutex::new(CellStateRegistry::new()),
            buff: new_statecell_buffer(init_buff_size),
            start_index: 0,
            end_index: 0,
            // it may be better if cvar was owned by each node
            // in that case, it would get notified when nodes were added
            cvar: Condvar::new(),
        }
    }
    // called by EPPS
    fn create_states(&mut self, n: usize) -> InteractionStoreSlice<'_> {
        let mut registry = self.registry.lock().unwrap();
        let mut indices: Vec<u32> = Vec::new();
        for i in 0..n {
            registry.push_back(CellState{
                locked: true,
                moved: false,
            });
            indices.push(self.end_index.wrapping_add(i as u32));
        }
        // if the size is not enough, we resize
        let size = self.end_index.wrapping_sub(self.start_index) as usize;
        if size > self.buff.len() {
            let new_buffer = new_statecell_buffer(self.buff.len() * 2);
            vec![InteractionCell::None; self.buff.len() * 2];
            // copy the states over
            let mut i = self.start_index;
            loop {
                if i == self.end_index {
                    break;
                }
                let old_idx = i as usize % self.buff.len();
                let new_idx = i as usize % new_buffer.len();
                copy_unsafecell(&self.buff[old_idx], &new_buffer[new_idx]);

                i = i.wrapping_add(1);
            }
            self.buff = new_buffer;
        }
        InteractionStoreSlice{
            parent: self,
            // borrowed buffer from parent
            buff: self.buff.clone(),
            indices,
        }
    }
    // fn get_states_neo(&mut self, packets: &mut [WavePacket]) -> InteractionStoreSlice<'_> {
    //     
    // }
    fn get_state_neo(&mut self, packet_slices: Vec<&mut Vec<WavePacket>>) -> InteractionStoreSlice<'_> {
        
    }
    fn get_states(&mut self, indices: Vec<u32>) -> InteractionStoreSlice<'_> {
        let mut registry = self.registry.lock().unwrap();
        loop {
            let mut all_free = true;
            for i in indices.iter().copied() {
                if registry.is_locked(i) {
                    all_free = false;
                    break;
                }
                let mut idx = i;
                loop {
                    if registry.is_locked(idx) {
                        all_free = false;
                        break;
                    } else if registry.is_moved(idx) {
                        let tombstone = if let InteractionCell::Tombstone(tombstone) = unsafe{&mut *self.buff[idx as usize].get()} {
                            tombstone
                        } else {
                            panic!("Expected a tombstone, but found something else!")
                        };
                        // tombstone.decrement_ref_cnt();
                        idx = tombstone.move_idx;
                        tombstone.ref_cnt -= 1;
                        if tombstone.ref_cnt == 0 {
                            set_unsafecell(&self.buff[idx as usize], InteractionCell::None);
                        }
                    } else {
                        // free and unlocked
                        // success!
                    }
                }
            }
            // let all_free = indices.iter().all(|&i|!registry.is_locked(i));
            
            if all_free {
                
                return InteractionStoreSlice{
                    parent: self,
                    buff: self.buff.clone(),
                    indices,
                }
            } else {
                registry = self.cvar.wait(registry).unwrap();
            }
        }
    }
    fn commit(&mut self) {

    }
}




// struct QuantumStateStoreHandle {
//     worker_id: u32,
//     owned: RcCache,
// }
// 
// impl QuantumStateStoreHandle {
//     
// }





struct ProcessNode {
    input_queue: Vec<Vec<WavePacket>>,
}

#[derive(Clone)]
pub struct WavePacket {
    pub t: u64,// ps
    pub t_spread: u32,// ps, three sigma
    pub wl: f32,
    pub wl_spread: f32,
    pub qs_handle: u32,
    pub snowflake: u32,
}
