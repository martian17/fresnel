#![allow(unused_imports)]
use smallvec::SmallVec;
use nalgebra::{SMatrix, Complex};
use arc_swap::{ArcSwap, Guard};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use crossbeam_utils::CachePadded;

// config and OpStore are stored under this file, 

use crate::concurrency::interaction_store::{
    InteractionStore,
};
use crate::types::core::{
    Time,
    BatchPolicy,
};
use crate::types::physics::PhotonicKrausOperators;

#[derive(Clone)]
pub struct SimulationConfig{
    pub batch: BatchPolicy,
}
use crate::nodes::epps::{
    EPPSBuilder,
};
use crate::nodes::single_port::{
    SinglePortBuilder,
};
use crate::nodes::dual_port::{
    DualPortBuilder,
};
use crate::nodes::spd::{
    SPDBuilder,
};




pub type OpStoreHandle = u16;

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

impl OperatorRecord {
    pub fn new() -> Self {
        Self {
            single: OpStore::new(),
            dual: OpStore::new(),
            epps: OpStore::new(),
        }
    }
}

pub struct BaseMetrics {
    pub packet_consumed: AtomicU64,
    pub packet_generated: AtomicU64,
    pub sim_time: AtomicU64,
}


pub struct SPDMetrics {
    pub base: BaseMetrics,
    pub clicks: AtomicU64,
    pub dark_counts: AtomicU64,
}


pub struct DualMetrics {
    pub base: BaseMetrics,
    pub merged_states: AtomicU64,
    pub paired_packets: AtomicU64,
    pub single_packets: AtomicU64,
    pub multi_packet_event: AtomicU64,
    pub pair_count: AtomicU64,
}

pub struct TelemetryRecord {
    pub epps: boxcar::Vec<CachePadded<BaseMetrics>>,
    pub single: boxcar::Vec<CachePadded<BaseMetrics>>,
    pub dual: boxcar::Vec<CachePadded<DualMetrics>>,
    pub spd: boxcar::Vec<CachePadded<SPDMetrics>>,
}


#[derive(Clone)]
pub struct SimulationContext {
    pub interaction_store: Arc<InteractionStore>,
    pub config: Arc<ArcSwap<SimulationConfig>>,
    pub operator_record: Arc<OperatorRecord>,
    pub telemetry: Arc<TelemetryRecord>,
}

impl SimulationContext {
    pub fn new() -> Self {
        SimulationContext {
            interaction_store: Arc::new(InteractionStore::new()),
            config: Arc::new(ArcSwap::from_pointee(SimulationConfig {
                batch: BatchPolicy{
                    period: 20_000_000,
                    max_size: 200,
                }})),
            operator_record: Arc::new(OperatorRecord::new()),
            telemetry: Arc::new(TelemetryRecord {
                epps: boxcar::Vec::new(),
                single: boxcar::Vec::new(),
                dual: boxcar::Vec::new(),
                spd: boxcar::Vec::new(),
            })
        }
    }
    pub fn update_config<F>(self, updater: F) -> Self
    where
        F: FnOnce(&mut SimulationConfig),
    {
        let mut config = (**self.config.load()).clone();
        updater(&mut config);
        self.config.store(Arc::new(config));
        self
    }
    pub fn epps(&self) -> EPPSBuilder {
        EPPSBuilder::new(self)
    }
    pub fn single(&self) -> SinglePortBuilder {
        SinglePortBuilder::new(self)
    }
    pub fn dual(&self) -> DualPortBuilder {
        DualPortBuilder::new(self)
    }
    pub fn spd(&self) -> SPDBuilder {
        SPDBuilder::new(self)
    }
}

// 
// 
// pub struct WaveProfile{
//     pub time_sigma: u32,
//     pub wavelength: f32,
//     pub wavelength_sigma: f32,
// }
// 
// pub struct EPPSBuilder {
//     pub signal_profile: WaveProfile,
//     pub idler_profile: WaveProfile,
//     pub pump_frequency: f64,
//     pub density_matrix: SMatrix<Complex<f32>, 4, 4>,
//     pub success_probability: f64,
//     pub context: &Arc<SimulationContext>
// }
// 
// 
// impl SimulationContext {
//     fn epps(self: &Arc<Self>) -> EPPSBuilder {
// 
//     }
// }
