#![allow(unused_imports)]
mod nodes;
mod concurrency;
mod types;
mod util;
mod collapser;
mod parquet;
mod quantum;

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
use crate::parquet::core::ParquetWorker;



// TODO: multi threaded store to get the operators (Original density matrix from EPPS, Kraus and S-Matrix)
// store.get(NodeId).
// loop through the past changes in the operators, find the latest one
// no garbage collection implemented, meaning operator changes will be stored indefinitely
// we should look into having them implemented based on the global clock (best effort latest item
// count up)


fn simple_epps_2spd() {
    let ctx = SimulationContext::new().update_config(|config| {
        config.batch.period = 20_000_000;
        config.batch.max_size = 200;
    });
    let epps = ctx.epps().spawn();
    let spd_1 = ctx.spd().id(1).spawn();
    let spd_2 = ctx.spd().id(2).spawn();

    // throughput monitor: prints wall-clock packets/second per SPD and the
    // simulation speed as % of real time (simulated ps advanced per wall-clock s)
    std::thread::spawn(move || {
        use std::sync::atomic::Ordering;
        let mut total: u64 = 0;
        let mut prev_sim_ps: u64 = 0;
        let start = std::time::Instant::now();
        for elapsed in 1u64.. {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let c1 = ctx.telemetry.spd[spd_1.worker_id.into()].base.packet_consumed.swap(0, Ordering::Relaxed);
            let c2 = ctx.telemetry.spd[spd_2.worker_id.into()].base.packet_consumed.swap(0, Ordering::Relaxed);
            total += c1 + c2;
            // overall progress is set by the laggard detector
            let sim_ps = ctx.telemetry.spd[spd_1.worker_id.into()].base.sim_time.load(Ordering::Relaxed).min(
                ctx.telemetry.spd[spd_2.worker_id.into()].base.sim_time.load(Ordering::Relaxed)
            );
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
    let ctx = SimulationContext::new();
    let epps_1 = ctx.epps().spawn();
    let epps_2 = ctx.epps().spawn();
    let bs_center = ctx.dual().spawn();
    // NOTE: in moonshot projects we seem to be using id 0 for special purposes,
    // so starting the ID from 1
    let spd_1 = ctx.spd().id(1).spawn();
    let spd_2 = ctx.spd().id(2).spawn();
    let spd_3 = ctx.spd().id(3).spawn();
    let spd_4 = ctx.spd().id(4).spawn();

    // throughput monitor: prints wall-clock packets/second per SPD and the
    // simulation speed as % of real time (simulated ps advanced per wall-clock s)
    std::thread::spawn(move || {
        use std::sync::atomic::Ordering;
        let mut total_packets: u64 = 0;
        let mut prev_sim_time: u64 = 0;
        let start = std::time::Instant::now();
        for elapsed in 1u64.. {
            std::thread::sleep(std::time::Duration::from_secs(1));
            // SPD telemetry
            let spd_packet_counts = ctx.telemetry.spd.iter().map(|(i, metrics)| {
                format!("{:>8}", metrics.base.packet_consumed.load(Ordering::Relaxed))
            }).collect::<Vec<_>>().join(" ");
            let sim_time = ctx.telemetry.spd.iter().map(|(i, metrics)| {
                metrics.base.sim_time.load(Ordering::Relaxed)
            }).min().unwrap();
            let time_dialation = (sim_time - prev_sim_time) as f64 / 1.0e12 * 100.0;
            let avg_realtime_pct = sim_time as f64 / start.elapsed().as_secs_f64() / 1.0e12 * 100.0;
            prev_sim_time = sim_time;
            let sim_time_seconds = sim_time as f64 / 1.0e12;
            total_packets += ctx.telemetry.spd.iter().map(|(i, metrics)| {
                metrics.base.packet_consumed.load(Ordering::Relaxed)
            }).sum::<u64>();
            println!(
                "[{elapsed:>4}s] SPD1-4: {spd_packet_counts} pkt/s | {time_dialation:>6.2}% of real time (avg {avg_realtime_pct:.2}%) | total: {total_packets} pkts, {sim_time_seconds:.3} sim-s",
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

fn main() {
    entanglement_swap();
    // simple_epps_2spd();
}
