#![allow(unused_imports)]
mod nodes;
mod concurrency;
mod types;
mod util;
mod collapser;
mod parquet;

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
use crate::types::physics::{
    PhotonicKrausOperators,
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
use crate::parquet::core::ParquetWorker;



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
            time_sigma: 75,
            wavelength: 1550.0,
            // TODO: Find a typical wavelength $\sigma$
            wavelength_sigma: 1.0,
        },
        idler_profile: WaveProfile{
            time_sigma: 75,
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

fn entanglement_swap(){
    let context = Arc::from(SimulationContext {
        interaction_store: Arc::from(InteractionStore::new()),
        config: ArcSwap::from_pointee(SimulationConfig {
            batch: BatchPolicy{
                period: 20_000_000,
                max_size: 200,
            }}),
        operator_record: OperatorRecord::new(),
    });
    let epps_1 = EPPSRunner::spawn(context.clone(), 0, EPPSTemplate{
        signal_profile: WaveProfile{
            time_sigma: 75,
            wavelength: 1550.0,
            wavelength_sigma: 1.0,
        },
        idler_profile: WaveProfile{
            time_sigma: 75,
            wavelength: 1550.0,
            wavelength_sigma: 1.0,
        },
        pump_frequency: 1.0E+9,
        density_matrix: outer_product(vector![Complex::ZERO, Complex::ONE, Complex::ONE, Complex::ZERO]),
        success_probability: 0.01,// 1% success rate results in 1.0E7 generations per second
    });
    let epps_2 = EPPSRunner::spawn(context.clone(), 0, EPPSTemplate{
        signal_profile: WaveProfile{
            time_sigma: 75,
            wavelength: 1550.0,
            wavelength_sigma: 1.0,
        },
        idler_profile: WaveProfile{
            time_sigma: 75,
            wavelength: 1550.0,
            wavelength_sigma: 1.0,
        },
        pump_frequency: 1.0E+9,
        density_matrix: outer_product(vector![Complex::ZERO, Complex::ONE, Complex::ONE, Complex::ZERO]),
        success_probability: 0.01,// 1% success rate results in 1.0E7 generations per second
    });
    // symmetric 50:50 beamsplitter, polarization preserving.
    // Mode basis: (left_H, left_V, right_H, right_V); the reflected path
    // picks up the i phase (BS ⊗ I_pol)
    let s = Complex::new(std::f32::consts::FRAC_1_SQRT_2, 0.0);
    let i = Complex::new(0.0, std::f32::consts::FRAC_1_SQRT_2);
    let z = Complex::new(0.0f32, 0.0);
    let bs_center = DualPortRunner::spawn(context.clone(), 2, DualPortTemplate {
        scattering_matrix: matrix![
            s, z, i, z;
            z, s, z, i;
            i, z, s, z;
            z, i, z, s;
        ],
    });
    // NOTE: in moonshot projects we seem to be using id 0 for special purposes,
    // so starting the ID from 1
    let spd_1_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_2_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_3_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_4_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_1_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_2_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_3_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let spd_4_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
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
    let spd_3 = SPDRunner::spawn(context.clone(), 1, SPDTemplate {
        spd_id: 3,
        packet_counter: spd_3_counter.clone(),
        sim_time_ps: spd_3_sim_time.clone(),
    });
    let spd_4 = SPDRunner::spawn(context.clone(), 1, SPDTemplate {
        spd_id: 4,
        packet_counter: spd_4_counter.clone(),
        sim_time_ps: spd_4_sim_time.clone(),
    });

    // throughput monitor: prints wall-clock packets/second per SPD and the
    // simulation speed as % of real time (simulated ps advanced per wall-clock s)
    std::thread::spawn(move || {
        use std::sync::atomic::Ordering;
        let counters = [spd_1_counter, spd_2_counter, spd_3_counter, spd_4_counter];
        let sim_times = [spd_1_sim_time, spd_2_sim_time, spd_3_sim_time, spd_4_sim_time];
        let mut total: u64 = 0;
        let mut prev_sim_ps: u64 = 0;
        let start = std::time::Instant::now();
        for elapsed in 1u64.. {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let c: Vec<u64> = counters.iter().map(|c| c.swap(0, Ordering::Relaxed)).collect();
            total += c.iter().sum::<u64>();
            // overall progress is set by the laggard detector
            let sim_ps = sim_times
                .iter()
                .map(|t| t.load(Ordering::Relaxed))
                .min()
                .unwrap();
            let realtime_pct =
                (sim_ps - prev_sim_ps) as f64 / 1.0e12 * 100.0;
            let avg_realtime_pct =
                sim_ps as f64 / start.elapsed().as_secs_f64() / 1.0e12 * 100.0;
            prev_sim_ps = sim_ps;
            println!(
                "[{elapsed:>4}s] SPD1-4: {:>8} {:>8} {:>8} {:>8} pkt/s | {realtime_pct:>6.2}% of real time (avg {avg_realtime_pct:.2}%) | total: {total} pkts, {:.3} sim-s",
                c[0], c[1], c[2], c[3],
                sim_ps as f64 / 1.0e12,
            );
        }
    });
    epps_1.exit_port(0).connect(spd_1.entry_port(0));
    epps_1.exit_port(1).connect(bs_center.entry_port(0));
    epps_2.exit_port(0).connect(bs_center.entry_port(1));
    epps_2.exit_port(1).connect(spd_4.entry_port(0));
    bs_center.exit_port(0).connect(spd_2.entry_port(0));
    bs_center.exit_port(1).connect(spd_3.entry_port(0));

    std::fs::create_dir_all("data").unwrap();
    let parquet_writer = ParquetWorker::spawn("data".into(), "entanglement_swap".to_string());
    spd_1.connect_time_tagger(parquet_writer.add_channel(spd_1.spd_id));
    spd_2.connect_time_tagger(parquet_writer.add_channel(spd_2.spd_id));
    spd_3.connect_time_tagger(parquet_writer.add_channel(spd_3.spd_id));
    spd_4.connect_time_tagger(parquet_writer.add_channel(spd_4.spd_id));
    parquet_writer.start();

    spd_1.start();
    spd_2.start();
    spd_3.start();
    spd_4.start();
    bs_center.start();
    epps_1.start();
    epps_2.start();

    spd_1.join();
    spd_2.join();
    spd_3.join();
    spd_4.join();
    bs_center.join();
    epps_1.join();
    epps_2.join();
}

fn hom_experiment(odl_arm_1_m: f64, odl_arm_2_m: f64) {
    let context = Arc::from(SimulationContext {
        interaction_store: Arc::from(InteractionStore::new()),
        config: ArcSwap::from_pointee(SimulationConfig {
            batch: BatchPolicy{
                period: 20_000_000,
                max_size: 200,
            }}),
        operator_record: OperatorRecord::new(),
    });
    // free-space ODL: arm length -> propagation delay at vacuum c
    let odl_delay_1_ps = (odl_arm_1_m / 299_792_458.0 * 1.0e12).round() as u64;
    let odl_delay_2_ps = (odl_arm_2_m / 299_792_458.0 * 1.0e12).round() as u64;
    let epps = EPPSRunner::spawn(context.clone(), 0, EPPSTemplate{
        signal_profile: WaveProfile{
            time_sigma: 75,
            wavelength: 1550.0,
            wavelength_sigma: 1.0,
        },
        idler_profile: WaveProfile{
            time_sigma: 75,
            wavelength: 1550.0,
            wavelength_sigma: 1.0,
        },
        pump_frequency: 1.0E+9,
        density_matrix: outer_product(vector![Complex::ZERO, Complex::ONE, Complex::ONE, Complex::ZERO]),
        success_probability: 0.01,// 1% success rate results in 1.0E7 generations per second
    });
    // lossless ODL: identity Jones matrix extended with the vacuum identity
    let mut odl_kraus = PhotonicKrausOperators::new();
    odl_kraus.push(SMatrix::identity());
    let odl_1 = SinglePortRunner::spawn(context.clone(), 1, SinglePortTemplate{
        kraus_operators: odl_kraus.clone(),
        delay: odl_delay_1_ps,
    });
    let odl_2 = SinglePortRunner::spawn(context.clone(), 1, SinglePortTemplate{
        kraus_operators: odl_kraus,
        delay: odl_delay_2_ps,
    });
    // symmetric 50:50 beamsplitter, polarization preserving.
    // Mode basis: (left_H, left_V, right_H, right_V); the reflected path
    // picks up the i phase (BS ⊗ I_pol)
    let s = Complex::new(std::f32::consts::FRAC_1_SQRT_2, 0.0);
    let i = Complex::new(0.0, std::f32::consts::FRAC_1_SQRT_2);
    let z = Complex::new(0.0f32, 0.0);
    let bs = DualPortRunner::spawn(context.clone(), 2, DualPortTemplate {
        scattering_matrix: matrix![
            s, z, i, z;
            z, s, z, i;
            i, z, s, z;
            z, i, z, s;
        ],
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
    epps.exit_port(0).connect(odl_1.entry_port(0));
    odl_1.exit_port(0).connect(bs.entry_port(0));
    epps.exit_port(1).connect(odl_2.entry_port(0));
    odl_2.exit_port(0).connect(bs.entry_port(1));
    bs.exit_port(0).connect(spd_1.entry_port(0));
    bs.exit_port(1).connect(spd_2.entry_port(0));

    std::fs::create_dir_all("data").unwrap();
    let parquet_writer = ParquetWorker::spawn("data".into(), "hom_experiment".to_string());
    spd_1.connect_time_tagger(parquet_writer.add_channel(spd_1.spd_id));
    spd_2.connect_time_tagger(parquet_writer.add_channel(spd_2.spd_id));
    parquet_writer.start();

    // SIGINT: flush and close the current parquet file (footer included),
    // then exit — the sim threads run forever and are reaped by process exit
    let mut parquet_writer = Some(parquet_writer);
    ctrlc::set_handler(move || {
        if let Some(writer) = parquet_writer.take() {
            eprintln!("SIGINT: closing parquet file...");
            writer.stop();
        }
        std::process::exit(0);
    }).unwrap();

    spd_1.start();
    spd_2.start();
    bs.start();
    odl_1.start();
    odl_2.start();
    epps.start();

    spd_1.join();
    spd_2.join();
    bs.join();
    odl_1.join();
    odl_2.join();
    epps.join();
}

fn arm_arg(flag: &str) -> f64 {
    std::env::args()
        .find_map(|arg| arg.strip_prefix(flag).map(str::to_owned))
        .map(|v| v.parse().unwrap_or_else(|_| panic!("{flag} expects a number (cm)")))
        .unwrap_or(0.0)
}

fn main() {
    let arm_1_cm = arm_arg("--arm1=");
    let arm_2_cm = arm_arg("--arm2=");
    println!("ODL arm lengths: {arm_1_cm} cm / {arm_2_cm} cm");
    hom_experiment(arm_1_cm / 100.0, arm_2_cm / 100.0);
    // entanglement_swap();
    // simple_epps_2spd();
}


// #![allow(unused_imports)]
// mod nodes;
// mod concurrency;
// mod types;
// mod util;
// mod collapser;
// 
// use std::sync::{Arc};
// use arc_swap::ArcSwap;
// use nalgebra::{
//     SMatrix,
//     SVector,
//     matrix,
//     vector,
//     Complex,
//     ComplexField,
// };
// use crate::concurrency::context::{
//     SimulationContext,
//     SimulationConfig,
//     OperatorRecord,
// };
// use crate::types::core::{
//     Time,
//     BatchPolicy,
// };
// use crate::concurrency::interaction_store::{
//     InteractionStore
// };
// 
// use crate::nodes::epps::{
//     EPPSRunner,
//     EPPSTemplate,
//     WaveProfile,
// };
// use crate::nodes::single_port::{
//     SinglePortRunner,
//     SinglePortTemplate,
// };
// use crate::nodes::dual_port::{
//     DualPortRunner,
//     DualPortTemplate,
// };
// use crate::nodes::spd::{
//     SPDRunner,
//     SPDTemplate,
// };
// use crate::nodes::core::{
//     NodeHandle,
// };
// 
// 
// 
// // TODO: multi threaded store to get the operators (Original density matrix from EPPS, Kraus and S-Matrix)
// // store.get(NodeId).
// // loop through the past changes in the operators, find the latest one
// // no garbage collection implemented, meaning operator changes will be stored indefinitely
// // we should look into having them implemented based on the global clock (best effort latest item
// // count up)
// 
// 
// pub fn outer_product<T, const D: usize>(v: SVector<T, D>) -> SMatrix<T, D, D>
// where
//     T: ComplexField,
// {
//     &v * v.adjoint()
// }
// 
// fn simple_epps_2spd() {
//     // TODO: Make a more human friendly context initialization interface
//     let context = Arc::from(SimulationContext {
//         interaction_store: Arc::from(InteractionStore::new()),
//         config: ArcSwap::from_pointee(SimulationConfig {
//             batch: BatchPolicy{
//                 period: 20_000_000,
//                 max_size: 200,
//             }}),
//         operator_record: OperatorRecord::new(),
//     });
//     let epps = EPPSRunner::spawn(context.clone(), 0, EPPSTemplate{
//         signal_profile: WaveProfile{
//             time_sigma: 75,
//             wavelength: 1550.0,
//             // TODO: Find a typical wavelength $\sigma$
//             wavelength_sigma: 1.0,
//         },
//         idler_profile: WaveProfile{
//             time_sigma: 75,
//             wavelength: 1550.0,
//             wavelength_sigma: 1.0,
//         },
//         pump_frequency: 1.0E+9,
//         density_matrix: outer_product(vector![Complex::ZERO, Complex::ONE, Complex::ONE, Complex::ZERO]),
//         success_probability: 0.01,// 1% success rate results in 1.0E7 generations per second
//     });
//     // NOTE: in moonshot projects we seem to be using id 0 for special purposes,
//     // so starting the ID from 1
//     let spd_1_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
//     let spd_2_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
//     let spd_1_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
//     let spd_2_sim_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
//     let spd_1 = SPDRunner::spawn(context.clone(), 1, SPDTemplate {
//         spd_id: 1,
//         packet_counter: spd_1_counter.clone(),
//         sim_time_ps: spd_1_sim_time.clone(),
//     });
//     let spd_2 = SPDRunner::spawn(context.clone(), 1, SPDTemplate {
//         spd_id: 2,
//         packet_counter: spd_2_counter.clone(),
//         sim_time_ps: spd_2_sim_time.clone(),
//     });
// 
//     // throughput monitor: prints wall-clock packets/second per SPD and the
//     // simulation speed as % of real time (simulated ps advanced per wall-clock s)
//     std::thread::spawn(move || {
//         use std::sync::atomic::Ordering;
//         let mut total: u64 = 0;
//         let mut prev_sim_ps: u64 = 0;
//         let start = std::time::Instant::now();
//         for elapsed in 1u64.. {
//             std::thread::sleep(std::time::Duration::from_secs(1));
//             let c1 = spd_1_counter.swap(0, Ordering::Relaxed);
//             let c2 = spd_2_counter.swap(0, Ordering::Relaxed);
//             total += c1 + c2;
//             // overall progress is set by the laggard detector
//             let sim_ps = spd_1_sim_time
//                 .load(Ordering::Relaxed)
//                 .min(spd_2_sim_time.load(Ordering::Relaxed));
//             let realtime_pct =
//                 (sim_ps - prev_sim_ps) as f64 / 1.0e12 * 100.0;
//             let avg_realtime_pct =
//                 sim_ps as f64 / start.elapsed().as_secs_f64() / 1.0e12 * 100.0;
//             prev_sim_ps = sim_ps;
//             println!(
//                 "[{elapsed:>4}s] SPD1: {c1:>9} pkt/s | SPD2: {c2:>9} pkt/s | {realtime_pct:>6.2}% of real time (avg {avg_realtime_pct:.2}%) | total: {total} pkts, {:.3} sim-s",
//                 sim_ps as f64 / 1.0e12,
//             );
//         }
//     });
//     epps.exit_port(0).connect(spd_1.entry_port(0));
//     epps.exit_port(1).connect(spd_2.entry_port(0));
//     spd_1.start();
//     spd_2.start();
//     epps.start();
// 
//     epps.join();
//     spd_1.join();
//     spd_2.join();
// }
// 
// fn main() {
//     simple_epps_2spd();
// }
