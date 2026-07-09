use std::sync::{Mutex, Condvar, Arc};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use smallvec::{SmallVec};

use crate::concurrency::registry::{
    CellState,
    CellStateRegistry,
};
use crate::nodes::core::WavePacket;
use crate::types::core::WrappingIterU32;


// make it 32 if it needs to support a larger optical circuit
type OpIndex = u16;
type NodeId = u16;
type PortId = u8;
type ExitPortId = u8;
type WpSnowflake = u32;
type Time = u64;



#[derive(Clone)]
pub enum Operator {
    EPPS {
        node: NodeId,
        time: Time,
        // todo: these could be possibly compacted to u32
        sink_left: (OpIndex, PortId),
        sink_right: (OpIndex, PortId),
    },
    Single {
        node: NodeId,
        time: Time,
        sink: (OpIndex, PortId),
    },
    // These represent 2x2 linear compoents
    // The worker thread then queries the actual Kraus oOperator and Scatter Matrix using
    // (NodeId, time)
    //
    // 2x2 component with interference. Photon incidence on both ports
    DualBivariate {
        node: NodeId,
        time: Time,
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
        time: Time,
        incidence_port_id: PortId,
        sink_left: (OpIndex, PortId),
        sink_right: (OpIndex, PortId),
    },
    SPD {
        node: NodeId,
        time: Time,
    },
}

impl Operator {
    // ExitPortId and PortId are different, since DualBivariate has 4 imaginary exit ports
    // Target port id is assumed known
    pub fn set_sink(&mut self, exit_port: ExitPortId, target: OpIndex) {
        match self {
            Operator::EPPS {sink_left, sink_right, ..} => {
                // sink_left: (OpIndex 0, PortId),
                // sink_right: (OpIndex 1, PortId),
                match exit_port {
                    0 => sink_left.0 = target,
                    1 => sink_right.0 = target,
                    _ => panic!("Operator::EPPS exit port out of range"),
                }
            },
            Operator::Single {sink, ..} => {
                // sink: (OpIndex 0, PortId),
                match exit_port {
                    0 => sink.0 = target,
                    _ => panic!("Operator::Single exit port out of range"),
                }
            },
            Operator::DualBivariate{sink_left, sink_right, ..} => {
                // sink_left: (OpIndex, OpIndex, PortId),
                // sink_right: (OpIndex, OpIndex, PortId),
                match exit_port {
                    0 => sink_left.0 = target,
                    1 => sink_left.1 = target,
                    2 => sink_right.0 = target,
                    3 => sink_right.1 = target,
                    _ => panic!("Operator::DualBivariate exit port out of range"),
                }
            },
            // 2x2 component without interference. One port at a time
            Operator::DualUnivariate {sink_left, sink_right, ..} => {
                // sink_left: (OpIndex, PortId),
                // sink_right: (OpIndex, PortId),
                match exit_port {
                    0 => sink_left.0 = target,
                    2 => sink_right.0 = target,
                    _ => panic!("Operator::DualUnivariate exit port out of range"),
                }
            },
            Operator::SPD{..} => {
                panic!("Operator::SPD should not have a sink port");
            },
        }
    }
}

// parameter tuned to be packed in 512 bytes
// further tuning may be necessary specific to experiments
#[derive(Clone)]
struct IslandOfInteraction {
    pub operators: SmallVec<[Operator; 13]>,
    // (wavepacket id, operator index, operator exit port identification)
    pub active_packets: ActivePacketStore,
    //SmallVec<[(u32, OpIndex, u8); 8]>
}

#[derive(Clone)]
pub struct ActivePacketStore {
    active_packets: SmallVec<[(WpSnowflake, OpIndex, ExitPortId); 8]>
}

impl ActivePacketStore {
    pub fn new() -> Self {
        Self {
            active_packets: SmallVec::new(),
        }
    }
    pub fn extract(&mut self, packet_id: WpSnowflake) -> (OpIndex, ExitPortId) {
        let mut match_index = self.active_packets.len();
        for i in 0..self.active_packets.len() {
            if self.active_packets[i].0 == packet_id {
                match_index = i;
                break;
            }
        }
        if match_index == self.active_packets.len() {
            // TODO: Better error and semantics
            panic!("Index not found!!");
        }
        let removed = self.active_packets.remove(match_index);
        (removed.1, removed.2)
    }
    pub fn push(&mut self, packet_id: WpSnowflake, op_index: OpIndex, port_index: u8){
        self.active_packets.push((packet_id, op_index, port_index));
    }
    pub fn is_empty(&self) -> bool {
        self.active_packets.is_empty()
    }
}

#[derive(Clone)]
struct Tombstone{
    // 256 qubits is more than too much to handle already
    // so u8 suffices
    ref_cnt: u8,
    move_destination: u32,
}

#[derive(Clone)]
enum WpResult {
    Empty {
        slot_handle: u32,
    },
    Success {
        time: u64,
        spd_id: NodeId,
        slot_handle: u32,
    }
}

#[derive(Clone)]
struct CollapseResult {
    // got some leeway, 20 packets would robably be overkill, but got 512 bytes of space
    packets: SmallVec<[WpResult; 20]>,
}


// this needs to be aligned to \pmod 128
#[derive(Clone)]
#[repr(align(128))]
pub enum InteractionCell {
    None,
    Tombstone(Tombstone),
    IslandOfInteraction(IslandOfInteraction),
    ComputeWip,
    Result(CollapseResult),
}


// multi threaded data structure that exposes relevant slices of data
pub struct InteractionStore{
    data: Mutex<StoreData>,
    cvar: Condvar,
}




impl InteractionStore {
    fn new() -> Self {
        let init_buff_size = 512;
        Self {
            data: Mutex::new(StoreData::with_capacity(init_buff_size)),
            // it may be better if cvar was owned by each node
            // in that case, it would get notified when nodes were added
            cvar: Condvar::new(),
        }
    }
    // states are created in batch, only by EPPS.
    // States are only merged, not created afterwards.
    pub fn create_states(self: &Arc<Self>, n: u32) -> InteractionStoreSlice {
        let mut data = self.data.lock().unwrap();
        // in-line realloc, since this is the only place where states are created
        data.suggest_realloc(n);

        let start_idx = data.registry.end_index();
        let end_idx = data.registry.end_index().wrapping_add(n);
        for handle in WrappingIterU32::new(start_idx, end_idx) {
            data.buff.unsafely_initialize_cell_with(handle, InteractionCell::None);
        }
        for _ in 0..n {
            data.registry.push_back(CellState::Locked);
        }
        InteractionStoreSlice{
            parent: self.clone(),
            buff: data.buff.clone(),
            indices: WrappingIterU32::new(start_idx, end_idx).collect(),
            retired: Vec::new(),
        }
    }

    pub fn get_states (self: &Arc<Self>, mut wp_batches: Vec<&mut Vec<WavePacket>>) -> InteractionStoreSlice {
        // first pass: check availability and update the moved states
        let mut data = self.cvar.wait_while(self.data.lock().unwrap(), |data| {
            for batch in wp_batches.iter_mut() {
                for wp in batch.iter_mut() {
                    // check if moved
                    // common case skips over this part
                    while data.registry.get(wp.state_handle) == CellState::Moved {
                        let InteractionCell::Tombstone(tombstone) = (unsafe { data.buff.get_mut(wp.state_handle) }) else {
                            panic!("InteractionStore data integrity fault: registry indicates tombstone, but found something else");
                        };
                        tombstone.ref_cnt -= 1;
                        let move_destination = tombstone.move_destination;
                        if tombstone.ref_cnt == 0 {
                            data.registry.set(wp.state_handle, CellState::Retired);
                            unsafe {
                                (*data.buff.cell_ptr(wp.state_handle)).assume_init_drop();
                            }
                        }
                        // now the tombstone is dropped, so using a copied value
                        // loops to the next iteration to check if it's still moved
                        wp.state_handle = move_destination;
                    }
                    // sanity check. At this point it should not be retired
                    debug_assert!(data.registry.get(wp.state_handle) != CellState::Retired);
                    if data.registry.get(wp.state_handle) == CellState::Locked {
                        // if true, then we loop all over again
                        return true;
                    }
                }
            }
            return false;
        }).unwrap();
        // second pass: claim the slots by locking them
        let mut indices: Vec<u32> = Vec::new();
        let mut previous_handle: Option<u32> = None;
        for batch in wp_batches.iter() {
            for wp in batch.iter() {
                if previous_handle == Some(wp.state_handle) || indices.contains(&wp.state_handle) {
                    continue;
                }
                previous_handle = Some(wp.state_handle);
                indices.push(wp.state_handle);
                data.registry.set(wp.state_handle, CellState::Locked);
            }
        }
        InteractionStoreSlice{
            parent: self.clone(),
            buff: data.buff.clone(),
            indices,
            retired: Vec::new(),
        }
    }
}

// TODO: This part requires more investigation
// unsafe impl Send for InteractionCell {}
// unsafe impl Sync for InteractionCell {}
// unsafe impl Send for InteractionStore {}
// unsafe impl Sync for InteractionStore {}
unsafe impl Send for StatecellBuffer {}
unsafe impl Sync for StatecellBuffer {}


pub struct StoreData {
    registry: CellStateRegistry,
    buff: Arc<StatecellBuffer>,
}

impl StoreData {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            registry: CellStateRegistry::with_capacity(capacity),
            buff: StatecellBuffer::new(capacity).into(),
        }
    }
    fn suggest_realloc(&mut self, additional_cnt: u32) {
        let current_len = self.registry.len();
        if current_len + additional_cnt > self.buff.capacity() as u32 {
            self.realloc(current_len + additional_cnt);
        }
    }
    fn realloc(&mut self, new_len: u32) {
        let new_capacity = new_len.next_power_of_two();
        let new_buff = StatecellBuffer::new(new_capacity as usize);
        let old_buff = &self.buff;
        let mut i = self.registry.start_index();
        while i != self.registry.end_index() {
            if self.registry.get(i) != CellState::Locked {
                // since the sectors are unwitten or already freed/distructed (which is relevant to
                // values stored in the heap by Smallvec<>, in case there was any overflow)
                // we do not need to call the distructor on the old values
                // and thus we use unsafe pointer copy (manual move)
                unsafe {
                    std::ptr::copy_nonoverlapping(old_buff.cell_ptr(i), new_buff.cell_ptr(i), 1);
                }
            }
            i = i.wrapping_add(1);
        }
        self.buff = new_buff.into();
        self.registry = self.registry.resized(new_capacity as usize);
    }
}


pub struct InteractionStoreSlice{
    parent: Arc<InteractionStore>,
    buff: Arc<StatecellBuffer>,
    indices: Vec<u32>,
    retired: Vec<u32>,
}

impl InteractionStoreSlice {
    pub fn retire(&mut self, handle: u32) {
        self.retired.push(handle);
    }
    pub fn get_mut(&self, handle: u32) -> &mut InteractionCell {
        unsafe {
            self.buff.get_mut(handle)
        }
    }
    // TODO: Add some methods so the nodes can access the cells
}

impl Drop for InteractionStoreSlice {
    fn drop(&mut self) {
        let mut data = self.parent.data.lock().unwrap();
        for i in self.indices.iter().copied() {
            data.registry.set(i, CellState::Free);
        }
        for i in self.retired.iter().copied() {
            data.registry.set(i, CellState::Retired);
            // Drops the cell. This frees up any vector or heap data that was referenced by the
            // cell. Since this is linear access at the front, it should be cheap enough
            // besides, the fact that the worker touched this means it's still likely to be on
            // cache
            unsafe {
                (*self.buff.cell_ptr(i)).assume_init_drop();
            }
        }
        for handle in data.registry.handles_from_front() {
            if data.registry.get(handle) != CellState::Retired {
                break;
            }
            data.registry.drop_front();
        }
        // if the buffer got realloced, move the results over
        if !Arc::ptr_eq(&data.buff, &self.buff) {
            let old_buff = &self.buff;
            let new_buff = &data.buff;
            for i in self.indices.iter().copied() {
                if data.registry.get(i) == CellState::Retired {
                    continue;
                }
                // since the relevant sectors on the new buffer are yet to be written
                // we do not need to call the distructor
                // and thus we use unsafe pointer copy (manual move)
                unsafe {
                    std::ptr::copy_nonoverlapping(old_buff.cell_ptr(i), new_buff.cell_ptr(i), 1);
                }
            }
        }
        self.parent.cvar.notify_all();
    }
}



// this struct originally contained Arc, but we are externalizing it for the sake of clarity
// in exchange we get double indirection, but it's worth it because this is not accessed
// in a hot loop
struct StatecellBuffer {
    buff: Box<[UnsafeCell<MaybeUninit<InteractionCell>>]>,
}

impl StatecellBuffer {
    fn new(size: usize) -> Self {
        Self {
            buff: (0..size)
                .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
                .collect(),
        }
    }
    fn to_index(&self, handle: u32) -> usize {
        handle as usize % self.buff.len()
    }
    fn cell_ptr(&self, handle: u32) -> *mut MaybeUninit<InteractionCell> {
        self.buff[self.to_index(handle)].get()
    }
    fn unsafely_initialize_cell_with(&self, handle: u32, cell: InteractionCell) {
        unsafe {
            *self.cell_ptr(handle) = MaybeUninit::new(cell);
        }
    }
    // unsafe fn get(&self, handle: u32) -> &InteractionCell {
    //     unsafe{(*self.cell_ptr(handle)).assume_init_ref()}
    // }
    // this one can be called twice in a row accidentally
    // marking it unsafe will make the danger more explicit
    unsafe fn get_mut(&self, handle: u32) -> &mut InteractionCell {
        unsafe{(*self.cell_ptr(handle)).assume_init_mut()}
    }
    fn capacity(&self) -> usize {
        return self.buff.len();
    }
}
