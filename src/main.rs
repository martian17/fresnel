#![allow(unused_imports)]
mod nodes;
mod concurrency;
mod types;
mod util;

use std::sync::{Arc};
use arc_swap::ArcSwap;
use nalgebra::{
    SMatrix,
    SVector,
    matrix,
    vector,
    Complex,
    ComplexField,
};
use crate::concurrency::context::{
    SimulationContext,
    SimulationConfig,
    OperatorRecord,
};
use crate::types::core::{
    Time,
    BatchPolicy,
};
use crate::concurrency::interaction_store::{
    InteractionStore
};

use crate::nodes::epps::{
    EPPSRunner,
    EPPSTemplate,
    WaveProfile,
};
use crate::nodes::single_port::{
    SinglePortRunner,
    SinglePortTemplate,
};
use crate::nodes::dual_port::{
    DualPortRunner,
    DualPortTemplate,
};
use crate::nodes::spd::{
    SPDRunner,
    SPDTemplate,
};
use crate::nodes::core::{
    NodeHandle,
};



// TODO: multi threaded store to get the operators (Original density matrix from EPPS, Kraus and S-Matrix)
// store.get(NodeId).
// loop through the past changes in the operators, find the latest one
// no garbage collection implemented, meaning operator changes will be stored indefinitely
// we should look into having them implemented based on the global clock (best effort latest item
// count up)


pub fn outer_product<T, const D: usize>(v: SVector<T, D>) -> SMatrix<T, D, D>
where
    T: ComplexField,
{
    &v * v.adjoint()
}

fn simple_epps_2spd() {
    // TODO: Make a more human friendly context initialization interface
    let context = Arc::from(SimulationContext {
        interaction_store: Arc::from(InteractionStore::new()),
        config: ArcSwap::from_pointee(SimulationConfig {
            batch: BatchPolicy{
                period: 20_000_000,
                max_size: 200,
            }}),
        operator_record: OperatorRecord::new(),
    });
    let epps = EPPSRunner::spawn(context.clone(), 0, EPPSTemplate{
        signal_profile: WaveProfile{
            time_sigma: 150,
            wavelength: 1550.0,
            // TODO: Find a typical wavelength $\sigma$
            wavelength_sigma: 1.0,
        },
        idler_profile: WaveProfile{
            time_sigma: 150,
            wavelength: 1550.0,
            wavelength_sigma: 1.0,
        },
        pump_frequency: 1.0E+9,
        density_matrix: outer_product(vector![Complex::ZERO, Complex::ONE, Complex::ONE, Complex::ZERO]),
        success_probability: 0.01,// 1% success rate results in 1.0E7 generations per second
    });
    // NOTE: in moonshot projects we seem to be using id 0 for special purposes,
    // so starting the ID from 1
    let spd_1_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_2_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_1_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_2_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_1 = SPDRunner::spawn(context.clone(), 1, SPDTemplate {
        spd_id: 1,
        packet_counter: spd_1_counter.clone(),
        sim_time_ps: spd_1_sim_time.clone(),
    });
    let spd_2 = SPDRunner::spawn(context.clone(), 1, SPDTemplate {
        spd_id: 2,
        packet_counter: spd_2_counter.clone(),
        sim_time_ps: spd_2_sim_time.clone(),
    });

    // throughput monitor: prints wall-clock packets/second per SPD and the
    // simulation speed as % of real time (simulated ps advanced per wall-clock s)
    std::thread::spawn(move || {
        use std::sync::atomic::Ordering;
        let mut total: u64 = 0;
        let mut prev_sim_ps: u64 = 0;
        let start = std::time::Instant::now();
        for elapsed in 1u64.. {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let c1 = spd_1_counter.swap(0, Ordering::Relaxed);
            let c2 = spd_2_counter.swap(0, Ordering::Relaxed);
            total += c1 + c2;
            // overall progress is set by the laggard detector
            let sim_ps = spd_1_sim_time
                .load(Ordering::Relaxed)
                .min(spd_2_sim_time.load(Ordering::Relaxed));
            let realtime_pct =
                (sim_ps - prev_sim_ps) as f64 / 1.0e12 * 100.0;
            let avg_realtime_pct =
                sim_ps as f64 / start.elapsed().as_secs_f64() / 1.0e12 * 100.0;
            prev_sim_ps = sim_ps;
            println!(
                "[{elapsed:>4}s] SPD1: {c1:>9} pkt/s | SPD2: {c2:>9} pkt/s | {realtime_pct:>6.2}% of real time (avg {avg_realtime_pct:.2}%) | total: {total} pkts, {:.3} sim-s",
                sim_ps as f64 / 1.0e12,
            );
        }
    });
    epps.exit_port(0).connect(spd_1.entry_port(0));
    epps.exit_port(1).connect(spd_2.entry_port(0));
    spd_1.start();
    spd_2.start();
    epps.start();

    epps.join();
    spd_1.join();
    spd_2.join();
}

fn main() {
    simple_epps_2spd();
}
