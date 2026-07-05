mod nodes;
mod concurrency;

// use std::sync::{Mutex, Condvar, Arc};
// use std::collections::VecDeque;
// use std::rc::{Rc};
// use std::cell::UnsafeCell;
// 
// type QuantumState = f64;
// 
// 
// #[derive(Clone)]
// struct Tombstone{
//     // 256 qubits is more than too much to handle already
//     // so u8 suffices
//     ref_cnt: u8,
//     move_idx: u32,
// }
// 
// 
// #[derive(Clone)]
// pub enum StateCell {
//     None,
//     Tombstone(Tombstone),
//     QuantumState(QuantumState),
// }
// 
// 
// 
// pub struct CellState {
//     pub locked: bool,
//     pub moved: bool,
// }
// 
// struct CellStateRegistry {
//     buff: Vec<u64>,
//     start_index: u32,
//     end_index: u32,
// }
// 
// impl CellStateRegistry {
//     fn new() -> Self {
//         Self {
//             buff: vec![0; 512],
//             start_index: 0,
//             end_index: 0,
//         }
//     }
//     fn is_locked(&self, i: u32) -> bool {
//         let idx = (i.div_euclid(32) as usize) % self.buff.len();
//         let offset = i.rem_euclid(32) << 1;
//         // operator precedence: >>, &, == (highest to lowest)
//         self.buff[idx] >> offset & 1 == 1
//     }
//     fn is_moved(&self, i: u32) -> bool {
//         let idx = (i.div_euclid(32) as usize) % self.buff.len();
//         let offset = i.rem_euclid(32) << 1 | 1;
//         // operator precedence: >>, &, == (highest to lowest)
//         self.buff[idx] >> offset & 1 == 1
//     }
//     fn get(&self, i: u32) -> CellState {
//         let idx = (i.div_euclid(32) as usize) % self.buff.len();
//         let offset = i.rem_euclid(32) << 1;
//         let n = self.buff[idx];
//         CellState {
//             locked: n >> offset & 1 == 1,
//             moved: n >> (offset | 1) & 1 == 1
//         }
//     }
//     fn set(&mut self, i: u32, state: CellState){
//         let idx = (i.div_euclid(32) as usize) % self.buff.len();
//         let offset = i.rem_euclid(32) << 1;
//         let mut n = self.buff[idx];
//         n &= !(0b11 << offset);
//         n |= (state.locked as u64 | (state.moved as u64) << 1) << offset;
//         self.buff[idx] = n;
//     }
//     fn lock(&mut self, i: u32){
//         let idx = (i.div_euclid(32) as usize) % self.buff.len();
//         let offset = i.rem_euclid(32) << 1;
//         let mut n = self.buff[idx];
//         n |= 0b01 << offset;
//         self.buff[idx] = n;
//     }
//     fn unlock(&mut self, i: u32){
//         let idx = (i.div_euclid(32) as usize) % self.buff.len();
//         let offset = i.rem_euclid(32) << 1;
//         let mut n = self.buff[idx];
//         n &= !(0b01 << offset);
//         self.buff[idx] = n;
//     }
//     fn push_back(&mut self, state: CellState){
//         let len = self.end_index.wrapping_sub(self.start_index) as usize;
//         let capacity = self.buff.len() * 32;
//         if len < capacity {
//             self.set(self.end_index, state);
//             self.end_index = self.end_index.wrapping_add(1);
//         } else {
//             let buff_len = self.buff.len();
//             // might want to use unsafe alloc in the future, if this becomes bottleneck, though unlikely
//             self.buff.resize(buff_len * 2, 0);
//             let old_head_idx = self.start_index.div_euclid(32) as usize % buff_len;
//             let old_tail_idx = self.end_index.div_euclid(32) as usize % buff_len;
//             let new_head_idx = self.start_index.div_euclid(32) as usize % (buff_len * 2);
//             // new_tail_idx ended up not being used in the commparison, but leaving it here just for
//             // the sake of completeness.
//             // let new_tail_idx = self.end_index.div_euclid(32) as usize % (buff_len * 2);
//             if old_head_idx == new_head_idx {
//                 // tail got unwrapped
//                 // +1 just to be safe. doesn't matter if junk gets copied. the range is captured by
//                 // start_index and end_index anyways.
//                 for i in 0..(old_tail_idx+1).min(buff_len) {
//                     self.buff[buff_len + i] = self.buff[i];
//                 }
//             } else {
//                 // head got shifted
//                 for i in old_head_idx..buff_len {
//                     self.buff[buff_len + i] = self.buff[i];
//                 }
//             }
//             self.set(self.end_index, state);
//             self.end_index = self.end_index.wrapping_add(1);
//         }
//     }
//     fn drop_front(&mut self) {
//         self.start_index = self.start_index.wrapping_add(1);
//     }
// }
// 
// 
// 
// pub struct QuantumStateStore {
//     registry: Mutex<CellStateRegistry>,
//     buff: Arc<[UnsafeCell<StateCell>]>,
// 
//     // virtual index inside the u32 space
//     start_index: u32,
//     end_index: u32,
//     cvar: Condvar,
// }
// 
// pub struct QuantumStates<'a> {
//     parent: &'a QuantumStateStore,
//     // borrowed buffer from parent
//     //buff: &'a mut 
//     buff: Arc<[UnsafeCell<StateCell>]>,
//     indices: Vec<u32>,
// }
// 
// impl<'a> Drop for QuantumStates<'a> {
//     fn drop(&mut self) {
//         let mut registry = self.parent.registry.lock().unwrap();
//         // if the buffer got realloced, move the results over
//         if !Arc::ptr_eq(&self.parent.buff, &self.buff) {
//             let old_buff = &self.buff;
//             let new_buff = &self.parent.buff;
//             let old_len = old_buff.len();
//             let new_len = new_buff.len();
//             for i in self.indices.iter().copied() {
//                 let old_idx = i as usize % old_len;
//                 let new_idx = i as usize % new_len;
//                 copy_unsafecell(&old_buff[old_idx], &new_buff[new_idx]);
//             }
//         }
//         for i in self.indices.iter().copied() {
//             registry.unlock(i);
//         }
//         self.parent.cvar.notify_all();
//     }
// }
// 
// 
// // unsafe fn get_mut<T>(ptr: &UnsafeCell<T>) -> &mut T {
// //   unsafe { &mut *ptr.get() }
// // }
// // 
// // fn get_shared<T>(ptr: &mut T) -> &UnsafeCell<T> {
// //   let t = ptr as *mut T as *const UnsafeCell<T>;
// //   // SAFETY: `T` and `UnsafeCell<T>` have the same memory layout
// //   unsafe { &*t }
// // }
// 
// fn new_statecell_buffer(size: usize) -> Arc<[UnsafeCell<StateCell>]>{
//     (0..size)
//         .map(|_| UnsafeCell::new(StateCell::None))
//         .collect::<Vec<_>>()
//         .into()
// }
// 
// fn copy_unsafecell<T>(src: &UnsafeCell<T>, dst: &UnsafeCell<T>) {
//     unsafe {
//         // according to Gemini, direct memory copy lets you copy things over without
//         // unwanted .drop() callings
//         core::ptr::copy_nonoverlapping(src.get(), dst.get(), 1);
//     }
// }
// 
// fn set_unsafecell<T>(dst: &UnsafeCell<T>, val: T) {
//     unsafe {
//         core::ptr::write(dst.get(), val);
//     }
// }
// 
// impl QuantumStateStore {
//     fn new() -> Self {
//         let init_buff_size = 512;
//         Self {
//             registry: Mutex::new(CellStateRegistry::new()),
//             buff: new_statecell_buffer(init_buff_size),
//             start_index: 0,
//             end_index: 0,
//             // it may be better if cvar was owned by each node
//             // in that case, it would get notified when nodes were added
//             cvar: Condvar::new(),
//         }
//     }
//     // called by EPPS
//     fn create_states(&mut self, n: usize) -> QuantumStates<'_> {
//         let mut registry = self.registry.lock().unwrap();
//         let mut indices: Vec<u32> = Vec::new();
//         for i in 0..n {
//             registry.push_back(CellState{
//                 locked: true,
//                 moved: false,
//             });
//             indices.push(self.end_index.wrapping_add(i as u32));
//         }
//         // if the size is not enough, we resize
//         let size = self.end_index.wrapping_sub(self.start_index) as usize;
//         if size > self.buff.len() {
//             let new_buffer = new_statecell_buffer(self.buff.len() * 2);
//             vec![StateCell::None; self.buff.len() * 2];
//             // copy the states over
//             let mut i = self.start_index;
//             loop {
//                 if i == self.end_index {
//                     break;
//                 }
//                 let old_idx = i as usize % self.buff.len();
//                 let new_idx = i as usize % new_buffer.len();
//                 copy_unsafecell(&self.buff[old_idx], &new_buffer[new_idx]);
// 
//                 i = i.wrapping_add(1);
//             }
//             self.buff = new_buffer;
//         }
//         QuantumStates{
//             parent: self,
//             // borrowed buffer from parent
//             buff: self.buff.clone(),
//             indices,
//         }
//     }
//     // fn get_states_neo(&mut self, packets: &mut [WavePacket]) -> QuantumStates<'_> {
//     //     
//     // }
//     fn get_states(&mut self, indices: Vec<u32>) -> QuantumStates<'_> {
//         let mut registry = self.registry.lock().unwrap();
//         loop {
//             let mut all_free = true;
//             for i in indices.iter().copied() {
//                 if registry.is_locked(i) {
//                     all_free = false;
//                     break;
//                 }
//                 let mut idx = i;
//                 loop {
//                     if registry.is_locked(idx) {
//                         all_free = false;
//                         break;
//                     } else if registry.is_moved(idx) {
//                         let tombstone = if let StateCell::Tombstone(tombstone) = unsafe{&mut *self.buff[idx as usize].get()} {
//                             tombstone
//                         } else {
//                             panic!("Expected a tombstone, but found something else!")
//                         };
//                         // tombstone.decrement_ref_cnt();
//                         idx = tombstone.move_idx;
//                         tombstone.ref_cnt -= 1;
//                         if tombstone.ref_cnt == 0 {
//                             set_unsafecell(&self.buff[idx as usize], StateCell::None);
//                         }
//                     } else {
//                         // free and unlocked
//                         // success!
//                     }
//                 }
//             }
//             // let all_free = indices.iter().all(|&i|!registry.is_locked(i));
//             
//             if all_free {
//                 return QuantumStates{
//                     parent: self,
//                     buff: self.buff.clone(),
//                     indices,
//                 }
//             } else {
//                 registry = self.cvar.wait(registry).unwrap();
//             }
//         }
//     }
//     fn commit(&mut self) {
// 
//     }
// }
// 
// 
// 
// 
// // struct QuantumStateStoreHandle {
// //     worker_id: u32,
// //     owned: RcCache,
// // }
// // 
// // impl QuantumStateStoreHandle {
// //     
// // }
// 
// 
// 
// pub struct WavePacket {
//     pub t: u64,// ps
//     pub t_spread: u32,// ps, three sigma
//     pub wl: f32,
//     pub wl_spread: f32,
//     pub qs_handle: u32,
//     pub snowflake: u32,
// }
// 
// 
// struct ProcessNode {
//     input_queue: Vec<Vec<WavePacket>>,
// }
// 
// 
// fn main() {
// }
