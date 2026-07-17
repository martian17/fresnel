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

use crate::types::core::{
    OpHandle,
    PortId,
    SinkModeId,
    WpSnowflake,
    Time,
    SinkModeLocation,
    ModeIndex,
};
use crate::util::set::U32OpenAddressSet;
use crate::concurrency::context::{
    OpStoreHandle,
};



#[derive(Clone)]
pub enum Operator {
    #[allow(clippy::upper_case_acronyms)]
    EPPS {
        store_handle: OpStoreHandle,
        time: Time,
        // todo: these could be possibly compacted to u32
        source_modes: [ModeIndex; 0],
        sink_modes: [ModeIndex; 2],
    },
    Single {
        store_handle: OpStoreHandle,
        time: Time,
        source_modes: [ModeIndex; 1],
        sink_modes: [ModeIndex; 1],
    },
    // These represent 2x2 linear compoents
    // The worker thread then queries the actual Kraus oOperator and Scatter Matrix using
    // (OpStoreHandle, time)
    //
    // 2x2 component with interference. Photon incidence on both ports
    DualBivariate {
        store_handle: OpStoreHandle,
        time: Time,
        // superficial similarties of the packets
        // teporal and frequential overlap (unit inner product)
        // max is 1.0
        packet_indistinguishability: f64,
        source_modes: [ModeIndex; 2],
        sink_modes: [ModeIndex; 4],
    },
    // 2x2 component without interference. One port at a time
    DualUnivariate {
        store_handle: OpStoreHandle,
        time: Time,
        incidence_port_id: PortId,
        source_modes: [ModeIndex; 1],
        sink_modes: [ModeIndex; 2],
    },
    #[allow(clippy::upper_case_acronyms)]
    SPD {
        id: u16,
        wp_snowflake: u32,
        time: Time,
        source_modes: [ModeIndex; 1],
        sink_modes: [ModeIndex; 0],
    },
    Dump {
        source_modes: [ModeIndex; 1],
        sink_modes: [ModeIndex; 0],
    },
}

impl Operator {
    // // SinkModeId and PortId are different, since DualBivariate has 4 imaginary exit ports
    // // Target port id is assumed known
    // pub fn set_sink(&mut self, exit_port: SinkModeId, target: OpHandle) {
    //     match self {
    //         Operator::EPPS {sink_signal, sink_idler, ..} => {
    //             // sink_signal: (OpHandle 0, PortId),
    //             // sink_idler: (OpHandle 1, PortId),
    //             match exit_port {
    //                 0 => sink_signal.0 = target,
    //                 1 => sink_idler.0 = target,
    //                 _ => panic!("Operator::EPPS exit port out of range"),
    //             }
    //         },
    //         Operator::Single {sink, ..} => {
    //             // sink: (OpHandle 0, PortId),
    //             match exit_port {
    //                 0 => sink.0 = target,
    //                 _ => panic!("Operator::Single exit port out of range"),
    //             }
    //         },
    //         Operator::DualBivariate{sink_left, sink_right, ..} => {
    //             // sink_left: (OpHandle, OpHandle, PortId),
    //             // sink_right: (OpHandle, OpHandle, PortId),
    //             match exit_port {
    //                 0 => sink_left.0 = target,
    //                 1 => sink_left.1 = target,
    //                 2 => sink_right.0 = target,
    //                 3 => sink_right.1 = target,
    //                 _ => panic!("Operator::DualBivariate exit port out of range"),
    //             }
    //         },
    //         // 2x2 component without interference. One port at a time
    //         Operator::DualUnivariate {sink_left, sink_right, ..} => {
    //             // sink_left: (OpHandle, PortId),
    //             // sink_right: (OpHandle, PortId),
    //             match exit_port {
    //                 0 => sink_left.0 = target,
    //                 2 => sink_right.0 = target,
    //                 _ => panic!("Operator::DualUnivariate exit port out of range"),
    //             }
    //         },
    //         Operator::SPD{..} => {
    //             panic!("Operator::SPD should not have a sink port");
    //         },
    //         Operator::Dump => {
    //             panic!("Operator::Dump should not have a sink port");
    //         },
    //     }
    // }
    pub fn clone_with_offset(&self, offset: ModeIndex) -> Operator {
        let mut cloned = self.clone();
        match &mut cloned {
            Operator::EPPS { sink_modes, .. } => {
                sink_modes[0] += offset;
                sink_modes[1] += offset;
            }
            Operator::Single { source_modes, sink_modes, .. } => {
                source_modes[0] += offset;
                sink_modes[0] += offset;
            }
            Operator::DualBivariate { source_modes, sink_modes, .. } => {
                source_modes[0] += offset;
                source_modes[1] += offset;
                sink_modes[0] += offset;
                sink_modes[1] += offset;
                sink_modes[2] += offset;
                sink_modes[3] += offset;
            }
            Operator::DualUnivariate { source_modes, sink_modes, .. } => {
                source_modes[0] += offset;
                sink_modes[0] += offset;
                sink_modes[1] += offset;
            }
            Operator::SPD { source_modes, .. } => {
                source_modes[0] += offset;
            },
            Operator::Dump { source_modes, .. } => {
                source_modes[0] += offset;
            }
        }
        cloned
    }
}

// parameter tuned to be packed in 512 bytes
// further tuning may be necessary specific to experiments
#[derive(Clone)]
pub struct IslandOfInteraction {
    pub operators: SmallVec<[Operator; 13]>,
    // (wavepacket id, operator index, operator exit port identification)
    pub active_packets: ActivePacketStore,
    //SmallVec<[(u32, OpHandle, u8); 8]>
    mode_max: u16,// mode_index < mode_max
}

// if active packets becomes 0, we dispatch 


impl IslandOfInteraction {
    pub fn new() -> Self {
        Self {
            operators: SmallVec::new(),
            active_packets: ActivePacketStore::new(),
            mode_max: 0,
        }
    }
    // pub fn set_sink(&mut self, sink_mode: SinkModeLocation, op_handle: OpHandle) {
    //     self.operators[sink_mode.operator as usize].set_sink(sink_mode.mode, op_handle);
    // }
    #[deprecated(note="Operators do not need to resolve to each other anymore, as interaction is kept track of virtually using [ModeIndex]")]
    pub fn add_operator(&mut self, operator: Operator) -> OpHandle {
        let len = self.operators.len();
        self.operators.push(operator);
        len as OpHandle
    }
    pub fn register_wavepacket(&mut self, wp: &WavePacket) -> ModeIndex {
        let mode_index = self.mode_max;
        self.active_packets.add(wp.snowflake, self.mode_max);
        self.mode_max += 1;
        mode_index
    }
}

#[derive(Clone)]
pub struct ActivePacketStore {
    active_packets: SmallVec<[(WpSnowflake, ModeIndex); 8]>,
}

impl ActivePacketStore {
    pub fn new() -> Self {
        Self {
            active_packets: SmallVec::new(),
        }
    }
    pub fn extract(&mut self, packet_id: WpSnowflake) -> ModeIndex {
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
        removed.1
    }
    pub fn add(&mut self, packet_id: WpSnowflake, mode_id: ModeIndex){
        self.active_packets.push((packet_id, mode_id));
    }
    pub fn is_empty(&self) -> bool {
        self.active_packets.is_empty()
    }
    pub fn len(&self) -> u8 {
        self.active_packets.len() as u8
    }
}

#[derive(Clone)]
pub struct Tombstone{
    // 256 qubits is more than too much to handle already
    // so u8 suffices
    ref_cnt: u8,
    move_destination: u32,
}

#[derive(Clone)]
pub enum WpResult {
    Empty {
        wp_snowflake: u32,
    },
    Success {
        time: u64,
        spd_id: OpStoreHandle,
        wp_snowflake: u32,
    }
}

impl WpResult {
    fn wp_snowflake(&self) -> u32 {
        match self {
            WpResult::Empty{wp_snowflake} => *wp_snowflake,
            WpResult::Success{wp_snowflake, ..} => *wp_snowflake,
        }
    }
}

#[derive(Clone)]
pub struct CollapseResult {
    // got some leeway, 20 packets would robably be overkill, but got 512 bytes of space
    pub packets: SmallVec<[WpResult; 20]>,
}

impl CollapseResult {
    fn get(&self, wp: &WavePacket) -> Option<Time> {
        let mut result: Option<&WpResult> = None;
        for res in self.packets.iter() {
            if res.wp_snowflake() == wp.snowflake {
                result = Some(res);
                break;
            }
        }
        let result = result.unwrap_or_else(||panic!("Wavepacket not found within result"));
        match result {
            WpResult::Empty{..} => None,
            WpResult::Success{..} => Some(wp.time),
        }
    }
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

impl InteractionCell {
    #[inline]
    pub fn unwrap_as_island_or_else<'a, F>(&'a mut self, f: F) -> &'a mut IslandOfInteraction
    where
        F: FnOnce(&Self) -> &'a mut IslandOfInteraction,
    {
        match self {
            InteractionCell::IslandOfInteraction(island) => island,
            _ => f(self),
        }
    }
}


// multi threaded data structure that exposes relevant slices of data
pub struct InteractionStore{
    data: Mutex<StoreData>,
    cvar: Condvar,
}




impl InteractionStore {
    pub fn new() -> Self {
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
    pub fn create_states(self: &Arc<Self>, n: u32) -> (InteractionStoreSlice, u32, u32) {
        let mut data = self.data.lock().unwrap();
        // in-line realloc, since this is the only place where states are created
        data.suggest_realloc(n);

        let start_idx = data.registry.end_index();
        let end_idx = data.registry.end_index().wrapping_add(n);
        for handle in WrappingIterU32::new(start_idx, end_idx) {
            // cell is either assume_init_dropped or garbage uninitialized
            // it is dangerous to call destructor on it. Could cause double free
            data.buff.unsafely_initialize_cell_with(handle, InteractionCell::None);
        }
        for _ in 0..n {
            data.registry.push_back(CellState::Locked);
        }
        (InteractionStoreSlice{
            parent: self.clone(),
            buff: data.buff.clone(),
            indices: WrappingIterU32::new(start_idx, end_idx).collect(),
            retired: Vec::new(),
            moved: Vec::new(),
        }, start_idx, end_idx)
    }

    // pub fn get_states (self: &Arc<Self>, mut wp_batches: Vec<&mut Vec<WavePacket>>) -> InteractionStoreSlice {
    pub fn get_states (self: &Arc<Self>, mut wp_batches: Vec<&mut Vec<WavePacket>>) -> InteractionStoreSlice {
        // first pass: check availability and update the moved states
        let mut data = self.cvar.wait_while(self.data.lock().unwrap(), |data| {
            for batch in wp_batches.iter_mut() {
                for wp in batch.iter_mut() {
                    // check if moved
                    // common case skips over this part
                    // this is necessary so that reserved states don't accidentally contain
                    // tombstone which could redirect the consumer to unreserved section of memory
                    while data.registry.get(wp.state_handle) == CellState::Moved {
                        let InteractionCell::Tombstone(tombstone) = (unsafe { data.buff.unsafely_get_mut(wp.state_handle) }) else {
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
                if previous_handle == Some(wp.state_handle) {
                    // heuristics to reduce the number of indices to be pushed
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
            moved: Vec::new(),
        }
    }
    pub fn get_states_from_raw_handles(self: &Arc<Self>, handles: &Vec<u32>) -> InteractionStoreSlice {
        // first pass: check availability and update the moved states
        let mut data = self.cvar.wait_while(self.data.lock().unwrap(), |data| {
            for state_handle in handles.iter() {
                if data.registry.get(*state_handle) == CellState::Locked {
                    return true;
                }
            }
            return false;
        }).unwrap();
        // second pass: claim the slots by locking them
        for state_handle in handles.iter() {
            data.registry.set(*state_handle, CellState::Locked);
        }
        InteractionStoreSlice{
            parent: self.clone(),
            buff: data.buff.clone(),
            indices: handles.clone(),
            retired: Vec::new(),
            moved: Vec::new(),
        }
    }
    pub fn get_collapsed_packets(self: &Arc<Self>, packets: &Vec<WavePacket>) -> Vec<Time> {
        let data = self.cvar.wait_while(self.data.lock().unwrap(), |data| {
            for wp in packets.iter() {
                let handle = wp.state_handle;
                if data.registry.get(wp.state_handle) == CellState::Locked {
                    return true;
                }
            }
            // every slots are unoccupied. Now we check if they are collapsed
            for wp in packets.iter() {
                let InteractionCell::Result(collapse_result) = (unsafe { data.buff.unsafely_get_mut(wp.state_handle) }) else {
                    return true;
                };
            }
            return false;
        }).unwrap();
        packets.iter().filter_map(|wp| {
            let InteractionCell::Result(collapse_result) = (unsafe { data.buff.unsafely_get_mut(wp.state_handle) }) else {
                panic!("Previous check a few lines ago should have caught this");
            };
            collapse_result.get(wp)
        }).collect()
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
    // TODO: Use U32OpenAddressSet here and compare the performance
    moved: Vec<u32>,
}

impl InteractionStoreSlice {
    pub fn retire(&mut self, handle: u32) {
        self.retired.push(handle);
    }
    pub fn get_mut(&mut self, handle: u32) -> &mut InteractionCell {
        unsafe {
            self.buff.unsafely_get_mut(handle)
        }
    }
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn unsafely_get_mut(&self, handle: u32) -> &mut InteractionCell {
        unsafe {
            self.buff.unsafely_get_mut(handle)
        }
    }
    // For cases where tombstone is created inside the same batch, and other wave packets
    // try to access the same state. (It was an island at the time of batch allocation, but
    // turned into tombstone as the batch process ran along)
    pub fn correct_state_handle_if_tombstone(&mut self, wp: &mut WavePacket) {
        let mut handle = wp.state_handle;
        // skipped in most cases
        while let InteractionCell::Tombstone(tombstone) = self.get_mut(handle) {
            let destination = tombstone.move_destination;
            tombstone.ref_cnt -= 1;
            if !self.moved.contains(&handle) {
                self.moved.push(handle);
            }
            handle = destination;
        }
        wp.state_handle = handle;
        // self.get_mut(handle).unwrap_as_island_or_else(|_| {
        //     panic!("Wavepacket does not point to island of interaction, or Tombstone move destination was not an island of interaction");
        // })
    }
    pub fn get_handles(&self) -> U32OpenAddressSet {
        let mut unique_indices = U32OpenAddressSet::new(32);
        for index in self.indices.iter() {
            unique_indices.insert(*index);
        }
        unique_indices
    }
    pub fn set(&mut self, handle: u32, cell: InteractionCell) {
        unsafe {
            *self.buff.unsafely_get_mut(handle) = cell;
        }
    }
    // NOTE: both donor and recipient are assumed to be IslandOfInteraction
    pub fn merge_islands(&mut self, donor_handle: u32, recipient_handle: u32){
        // NOTE: donor_handle and recipient_handle are provably disjoint,
        // so we get multiple mutable borrows using unsafe
        let donor_cell = unsafe{self.unsafely_get_mut(donor_handle)};
        let donor = donor_cell.unwrap_as_island_or_else(|_|{
            panic!("Merge failed. Donor is not IslandOfInteraction");
        });
        let recipient = unsafe{self.unsafely_get_mut(recipient_handle)}.unwrap_as_island_or_else(|_|{
            panic!("Merge failed. Recipient is not IslandOfInteraction");
        });
        let offset = recipient.mode_max;
        for operator in donor.operators.iter() {
            recipient.operators.push(operator.clone_with_offset(offset));
        }
        for (packet_id, mode_idx) in donor.active_packets.active_packets.iter() {
            recipient.active_packets.add(*packet_id, mode_idx + offset);
        }
        recipient.mode_max += donor.mode_max;
        *donor_cell = InteractionCell::Tombstone(Tombstone {
            ref_cnt: donor.active_packets.len(),
            move_destination: recipient_handle,
        });
        self.moved.push(donor_handle);
    }
    // TODO: Add some methods so the nodes can access the cells
}

impl Drop for InteractionStoreSlice {
    fn drop(&mut self) {
        let mut data = self.parent.data.lock().unwrap();
        for i in self.indices.iter().copied() {
            data.registry.set(i, CellState::Free);
        }
        // Handle move and retire. These should refer to self.buff, and
        // the parent.data.buff should only be touched in the moved branch
        // at last.
        for moved_handle in self.moved.iter().copied() {
            let InteractionCell::Tombstone(tombstone) = (unsafe { self.buff.unsafely_get_mut(moved_handle) }) else {
                panic!("InteractionStore data integrity fault: moved handle must contain a tombstone");
            };
            if tombstone.ref_cnt == 0 {
                data.registry.set(moved_handle, CellState::Retired);
                unsafe {
                    (*self.buff.cell_ptr(moved_handle)).assume_init_drop();
                }
            } else {
                data.registry.set(moved_handle, CellState::Moved);
            }
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
            // since indices could be duplicated, so this could run multiple times per index
            // but it shouldn't be too bad
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
    #[allow(clippy::mut_from_ref)]
    unsafe fn unsafely_get_mut(&self, handle: u32) -> &mut InteractionCell {
        unsafe{(*self.cell_ptr(handle)).assume_init_mut()}
    }
    fn capacity(&self) -> usize {
        return self.buff.len();
    }
}
