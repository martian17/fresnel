#![allow(unused_imports)]
use smallvec::SmallVec;
use nalgebra::{SMatrix, Complex};
use arc_swap::{ArcSwap, Guard};
use std::sync::Arc;

// config and OpStore are stored under this file, 

use crate::concurrency::interaction_store::{
    InteractionStore,
};
use crate::types::core::{
    Time,
    BatchPolicy,
};
use crate::types::physics::PhotonicKrausOperators;



pub struct SimulationConfig{
    pub batch: BatchPolicy,
}




type OpStoreHandle = u16;

// TODO: OpStore currently only serves as a table.
// However, in the future, we will need to store the past operators, and 
// interpolate between them. Also we need a mechanism to invalidate and
// remove the old values from the store, based on best effort clock. In
// addition, some experiment will require that the operator follows a 
// specific function based on time. To this end, this data structure necessitates
// a further architectural consideration
pub struct OpStore<T> {
    items: boxcar::Vec<ArcSwap<T>>,
}

impl<T> OpStore<T> {
    pub fn new() -> Self {
        Self { items: boxcar::Vec::new() }
    }

    pub fn add(&self, operator: T) -> OpStoreHandle {
        self.items.push(ArcSwap::from_pointee(operator)) as OpStoreHandle
    }

    pub fn set(&self, handle: OpStoreHandle, operator: T, _time: Time) {
        self.items[handle as usize].store(Arc::new(operator));
    }

    pub fn get(&self, handle: OpStoreHandle, _time: Time) -> Guard<Arc<T>> {
        self.items[handle as usize].load()
    }
}




pub struct OperatorRecord {
    pub single: OpStore<PhotonicKrausOperators>, // [3x3 kraus operator; <=7]
    pub dual: OpStore<SMatrix<Complex<f32>, 4, 4>>, // 4x4 scatter matrix
    pub epps: OpStore<SMatrix<Complex<f32>, 4, 4>>, // 4x4 density matrix
}

// this shall be enclosed in Arc, and all the constituants be thread safe
pub struct SimulationContext {
    pub interaction_store: Arc<InteractionStore>,
    pub config: ArcSwap<SimulationConfig>,
    pub operator_record: OperatorRecord,
}
