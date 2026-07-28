mod nodes;
mod concurrency;
mod types;
mod util;
mod collapser;
mod parquet;
mod quantum;

use crate::concurrency::context::{
    SimulationContext,
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

fn monitor_throughput(ctx: SimulationContext){
    // throughput monitor: prints wall-clock packets/second per SPD and the
    // simulation speed as % of real time (simulated ps advanced per wall-clock s)
    std::thread::spawn(move || {
        use std::sync::atomic::Ordering;
        let mut total_historical_packets: u64 = 0;
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
            let spd_count = ctx.telemetry.spd.count();


            // total packet telemetry
            let total_packets: u64 = ctx.telemetry.single.iter().map(|(i, metrics)| {
                metrics.packet_consumed.load(Ordering::Relaxed)
            }).sum::<u64>() + 
            ctx.telemetry.dual.iter().map(|(i, metrics)| {
                metrics.base.packet_consumed.load(Ordering::Relaxed)
            }).sum::<u64>() + 
            ctx.telemetry.spd.iter().map(|(i, metrics)| {
                metrics.base.packet_consumed.load(Ordering::Relaxed)
            }).sum::<u64>();

            total_historical_packets += total_packets;

            // reset it before the next round
            for (i, record) in ctx.telemetry.single.iter(){
                record.packet_consumed.store(0, Ordering::Relaxed);
            }
            for (i, record) in ctx.telemetry.dual.iter(){
                record.base.packet_consumed.store(0, Ordering::Relaxed);
            }
            for (i, record) in ctx.telemetry.spd.iter(){
                record.base.packet_consumed.store(0, Ordering::Relaxed);
            }
            println!(
                "[{elapsed:>4}s] All: {total_packets} pkt/s | SPD1-{spd_count}: {spd_packet_counts} pkt/s | {time_dialation:>6.2}% of real time (avg {avg_realtime_pct:.2}%) | total: {total_historical_packets} pkts, {sim_time_seconds:.3} sim-s",
            );
        }
    });
}

fn simple_epps_2spd() {
    let ctx = SimulationContext::new().update_config(|config| {
        config.batch.period = 20_000_000;
        config.batch.max_size = 200;
    });
    let epps = ctx.epps().spawn();
    let spd_1 = ctx.spd().id(1).spawn();
    let spd_2 = ctx.spd().id(2).spawn();

    monitor_throughput(ctx.clone());

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
    let ctx = SimulationContext::new().update_config(|config| {
        // config.batch.period = 2_000_000_000;
        // config.batch.max_size = 20000;
        config.batch.period = 20_000_000;
        config.batch.max_size = 200;
    });
    let epps_1 = ctx.epps().spawn();
    let epps_2 = ctx.epps().spawn();
    let bs_center = ctx.dual().spawn();
    // NOTE: in moonshot projects we seem to be using id 0 for special purposes,
    // so starting the ID from 1
    let spd_1 = ctx.spd().id(1).spawn();
    let spd_2 = ctx.spd().id(2).spawn();
    let spd_3 = ctx.spd().id(3).spawn();
    let spd_4 = ctx.spd().id(4).spawn();

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

    monitor_throughput(ctx.clone());

    let mut parquet_writer = Some(parquet_writer);
    ctrlc::set_handler(move || {
        if let Some(writer) = parquet_writer.take() {
            eprintln!("SIGINT: closing parquet file...");
            writer.stop();
            std::process::exit(0);
        }
    }).unwrap();

    // TODO: ctx.start_all() and ctx.join_all()
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
    let ctx = SimulationContext::new();
    let epps = ctx.epps().spawn();

    // free-space ODL: arm length -> propagation delay at vacuum c
    let odl_1 = ctx.single().delay(
        (odl_arm_1_m / 299_792_458.0 * 1.0e12).round() as u64
    ).spawn();
    let odl_2 = ctx.single().delay(
        (odl_arm_2_m / 299_792_458.0 * 1.0e12).round() as u64
    ).spawn();

    let bs = ctx.dual().spawn();
    let spd_1 = ctx.spd().id(1).spawn();
    let spd_2 = ctx.spd().id(2).spawn();
    
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

    monitor_throughput(ctx.clone());

    // SIGINT: flush and close the current parquet file (footer included),
    // then exit — the sim threads run forever and are reaped by process exit
    let mut parquet_writer = Some(parquet_writer);
    ctrlc::set_handler(move || {
        if let Some(writer) = parquet_writer.take() {
            eprintln!("SIGINT: closing parquet file...");
            writer.stop();
            std::process::exit(0);
        }
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
