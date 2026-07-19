use crate::concurrency::interaction_store::{
    IslandOfInteraction,
    CollapseResult,
    WpResult,
    Operator,
};
use crate::types::core::{
    ModeIndex,
    Time,
};

use rand::rng;
use smallvec::SmallVec;

pub fn mock_collapse(island: &IslandOfInteraction) -> CollapseResult {
    let collapsed_packets: SmallVec<[WpResult; 20]> = island.operators.iter().filter_map(|op| match op {
        Operator::SPD{id, time, wp_snowflake, ..} => Some(WpResult::Success{
            time: *time,
            spd_id: *id,
            wp_snowflake: *wp_snowflake,
        }),
        _ => None,
    }).collect();
    let ref_cnt: usize = collapsed_packets.len();
    CollapseResult {
        packets: collapsed_packets,
        ref_cnt,
    }
}

#[derive(Clone, Debug)]
enum MonotonicMapCell<T>{
    Data(T),
    Moved(u16),
}

#[derive(Debug)]
struct MonotonicMap<T>{
    data: Vec<MonotonicMapCell<T>>,
}

impl<T: Clone> MonotonicMap<T>{
    fn new() -> Self {
        Self {
            data: Vec::new(),
        }
    }
    fn add(&mut self, value: T) -> u16 {
        let index = self.data.len();
        self.data.push(MonotonicMapCell::Data(value));
        return index as u16;
    }
    fn get(&mut self, mut handle: u16) -> &mut T {
        while let MonotonicMapCell::Moved(next_handle) = self.data[handle as usize] {
            handle = next_handle;
        }
        match &mut self.data[handle as usize] {
            MonotonicMapCell::Data(data) => data,
            _ => unreachable!("actually unreachable")
        }
    }
    fn move_out_to(&mut self, mut handle: u16, next_handle: u16) -> T {
        while let MonotonicMapCell::Moved(next_handle) = self.data[handle as usize] {
            handle = next_handle;
        }
        let data = match &mut self.data[handle as usize] {
            MonotonicMapCell::Data(data) => data,
            _ => unreachable!("actually unreachable")
        };
        let cloned = data.clone();
        self.data[handle as usize] = MonotonicMapCell::Moved(next_handle);
        return cloned;
    }
}

#[derive(Clone, Copy, Debug)]
enum Photon{
    Single,
    Vac,
}

impl Photon {
    fn is_vac(&self) -> bool {
        match self {
            Photon::Single => false,
            Photon::Vac => true,
        }
    }
    fn is_single(&self) -> bool {
        match self {
            Photon::Single => true,
            Photon::Vac => false,
        }
    }
}

#[derive(Clone, Debug)]
enum MonteCarloOperator{
    Single {
        source: ModeIndex,
        sink: ModeIndex,
    },
    Double {
        source: ModeIndex,
        sinks: [ModeIndex; 2],
    },
    DoubleInterfering {
        indistinguishability: f64,
        sources: [ModeIndex; 2],
        source_photons: [Option<Photon>; 2],
        sinks: [ModeIndex; 4],
    },
    MultiModeMap(MultiModeMap),
    #[allow(clippy::upper_case_acronyms)]
    SPD {
        source: ModeIndex,
        id: u16,
        wp_snowflake: u32,
        time: Time,
    },
    Dump {
        source: ModeIndex,
    },

}

#[derive(Clone, Debug)]
struct MultiModeMap{
    source_photons: Vec<Option<Photon>>,
    mode_map: Vec<(ModeIndex, ModeIndex, ModeIndex)>,
}

impl MultiModeMap {
    fn new() -> Self {
        Self {
            source_photons: Vec::new(),
            mode_map: Vec::new(),
        }
    }
    fn add(&mut self, entry: (ModeIndex, ModeIndex, ModeIndex)) {
        for entry_1 in self.mode_map.iter() {
            if entry_1.0 == entry.0 {
                return;
            }
        }
        self.mode_map.push(entry);
        self.source_photons.push(None);
    }
    fn is_ready(&self) -> bool {
        self.source_photons.iter().all(|p|p.is_some())
    }
    fn register_photon(&mut self, mode: u16, photon: Photon) {
        let mut idx = 0;
        for i in 0..self.mode_map.len() {
            if self.mode_map[i].0 == mode {
                idx = i;
                break;
            }
        }
        self.source_photons[idx] = Some(photon);
    }
}


fn multi_mode_map_append(mode_map: &mut Vec<(ModeIndex, ModeIndex, ModeIndex)>, entry: (ModeIndex, ModeIndex, ModeIndex)) {
    for entry_1 in mode_map.iter() {
        if entry_1.0 == entry.0 {
            return;
        }
    }
    mode_map.push(entry);
}

pub fn collapse(island: &IslandOfInteraction) -> CollapseResult {
    let mut result = CollapseResult{
        ref_cnt: 0,
        packets: SmallVec::new(),
    };
    let mut operators = MonotonicMap::<MonteCarloOperator>::new();
    let mut mode_operator_map: Vec<u16> = (0..island.mode_max).map(|_|u16::MAX).collect(); // maps mode index to operator index
    
    let mut active_modes: Vec<(u16, Photon)> = Vec::new();
    for op in island.operators.iter() {
        match op {
            Operator::EPPS {source_modes, sink_modes, ..} => {
                active_modes.push((sink_modes[0], Photon::Single));
                active_modes.push((sink_modes[1], Photon::Single));
            },
            Operator::Single {source_modes, sink_modes, ..} => {
                mode_operator_map[source_modes[0] as usize] = operators.add(MonteCarloOperator::Single{
                    source: source_modes[0],
                    sink: sink_modes[0],
                });
            },
            Operator::DualBivariate {source_modes, sink_modes, packet_indistinguishability, ..} => {
                // this one could be multimap
                let mode_1 = mode_operator_map[source_modes[0] as usize];
                let mode_2 = mode_operator_map[source_modes[1] as usize];
                if mode_1 != u16::MAX {
                    let present_operator_1 = operators.get(mode_1);
                    match present_operator_1 {
                        MonteCarloOperator::DoubleInterfering{sources, sinks, ..} => {
                            let mut multi_map = MultiModeMap::new();
                            multi_map.add((sources[0], sinks[0], sinks[1]));
                            multi_map.add((sources[1], sinks[2], sinks[3]));
                            multi_map.add((source_modes[0], sink_modes[0], sink_modes[1]));
                            multi_map.add((source_modes[1], sink_modes[2], sink_modes[3]));
                            *present_operator_1 = MonteCarloOperator::MultiModeMap(multi_map);
                        },
                        MonteCarloOperator::MultiModeMap(multi_map) => {
                            multi_map.add((source_modes[0], sink_modes[0], sink_modes[1]));
                            multi_map.add((source_modes[1], sink_modes[2], sink_modes[3]));
                        },
                        _ => unreachable!("unreachable")
                    }
                    if mode_2 != u16::MAX {
                        let present_operator_2 = operators.move_out_to(mode_2, mode_1);
                        let present_operator_1 = operators.get(mode_1);
                        match present_operator_1 {
                            MonteCarloOperator::MultiModeMap(multi_map) => {
                                match present_operator_2 {
                                    MonteCarloOperator::DoubleInterfering{sources, sinks, ..} => {
                                        multi_map.add((sources[0], sinks[0], sinks[1]));
                                        multi_map.add((sources[1], sinks[2], sinks[3]));
                                    },
                                    MonteCarloOperator::MultiModeMap(moved_multi_map) => {
                                        for entry in moved_multi_map.mode_map.iter() {
                                            multi_map.add(*entry);
                                        }
                                    },
                                    _ => unreachable!("unreachable")
                                }
                                
                            }
                            _ => unreachable!("unreachable")
                        }
                    } else {
                        mode_operator_map[source_modes[1] as usize] = mode_1;
                    }
                } else if mode_2 != u16::MAX {
                    let present_operator_2 = operators.get(mode_2);
                    match present_operator_2 {
                        MonteCarloOperator::DoubleInterfering{sources, sinks, ..} => {
                            let mut multi_map = MultiModeMap::new();
                            multi_map.add((sources[0], sinks[0], sinks[1]));
                            multi_map.add((sources[1], sinks[2], sinks[3]));
                            multi_map.add((source_modes[0], sink_modes[0], sink_modes[1]));
                            multi_map.add((source_modes[1], sink_modes[2], sink_modes[3]));
                            *present_operator_2 = MonteCarloOperator::MultiModeMap(multi_map);
                        },
                        MonteCarloOperator::MultiModeMap(multi_map) => {
                            multi_map.add((source_modes[0], sink_modes[0], sink_modes[1]));
                            multi_map.add((source_modes[1], sink_modes[2], sink_modes[3]));
                        },
                        _ => unreachable!("unreachable")
                    }
                    mode_operator_map[source_modes[0] as usize] = mode_2;
                } else {
                    // no collision, phew.
                    let handle = operators.add(MonteCarloOperator::DoubleInterfering{
                        sources: *source_modes,
                        source_photons: [None, None],
                        sinks: *sink_modes,
                        indistinguishability: *packet_indistinguishability,
                    });
                    mode_operator_map[source_modes[0] as usize] = handle;
                    mode_operator_map[source_modes[1] as usize] = handle;
                }
            },
            Operator::DualUnivariate {source_modes, sink_modes, ..} => {
                mode_operator_map[source_modes[0] as usize] = operators.add(MonteCarloOperator::Double{
                    source: source_modes[0],
                    sinks: [sink_modes[0], sink_modes[1]],
                });
            },
            Operator::SPD {source_modes, sink_modes, wp_snowflake, id, time, ..} => {
                mode_operator_map[source_modes[0] as usize] = operators.add(MonteCarloOperator::SPD{
                    id: *id,
                    time: *time,
                    source: source_modes[0],
                    wp_snowflake: *wp_snowflake,
                });
            },
            Operator::Dump {source_modes, sink_modes, ..} => {
                mode_operator_map[source_modes[0] as usize] = operators.add(MonteCarloOperator::Dump{
                    source: source_modes[0],
                });
            },
        }
    }

    for i in mode_operator_map.iter() {
        if *i == u16::MAX {
            println!("MAX detected!!");
            println!("{:?}, {:?}, {:?}, {:?}", island, operators, mode_operator_map, active_modes);
        }
    }

    // all right, now we've constructed mode -> operators map
    while let Some((mode, photon)) = active_modes.pop() {
        match operators.get(mode_operator_map[mode as usize]) {
            MonteCarloOperator::Single {source, sink } => {
                active_modes.push((*sink, photon));
            },
            MonteCarloOperator::Double {source, sinks} => {
                if rand::random_bool(0.5) {
                    active_modes.push((sinks[0], photon));
                    active_modes.push((sinks[1], Photon::Vac));
                } else {
                    active_modes.push((sinks[0], Photon::Vac));
                    active_modes.push((sinks[1], photon));
                }
            },
            MonteCarloOperator::DoubleInterfering { indistinguishability, sources, source_photons, sinks} => {
                if mode == sources[0] {
                    source_photons[0] = Some(photon);
                } else {
                    source_photons[1] = Some(photon);
                }
                if source_photons[0].is_none() || source_photons[1].is_none() {
                    continue;
                }
                let left = source_photons[0].unwrap();
                let right = source_photons[1].unwrap();
                if left.is_single() && right.is_single() && rand::random_bool(*indistinguishability){
                    if rand::random_bool(0.5) {
                        active_modes.push((sinks[0], Photon::Single));
                        active_modes.push((sinks[1], Photon::Vac));
                        active_modes.push((sinks[2], Photon::Single));
                        active_modes.push((sinks[3], Photon::Vac));
                    } else {
                        active_modes.push((sinks[0], Photon::Vac));
                        active_modes.push((sinks[1], Photon::Single));
                        active_modes.push((sinks[2], Photon::Vac));
                        active_modes.push((sinks[3], Photon::Single));
                    }
                } else {
                    if rand::random_bool(0.5) {
                        active_modes.push((sinks[0], left));
                        active_modes.push((sinks[1], Photon::Vac));
                    } else {
                        active_modes.push((sinks[0], Photon::Vac));
                        active_modes.push((sinks[1], left));
                    }
                    if rand::random_bool(0.5) {
                        active_modes.push((sinks[2], right));
                        active_modes.push((sinks[3], Photon::Vac));
                    } else {
                        active_modes.push((sinks[2], Photon::Vac));
                        active_modes.push((sinks[3], right));
                    }
                }
            },
            MonteCarloOperator::MultiModeMap(multi_mode_map) => {
                multi_mode_map.register_photon(mode, photon);
                if !multi_mode_map.is_ready() {
                    continue;
                }
                for ((source, sink_left, sink_right), photon) in multi_mode_map.mode_map.iter().zip(multi_mode_map.source_photons.iter()) {
                    let photon = photon.unwrap();
                    if rand::random_bool(0.5) {
                        active_modes.push((*sink_left, photon));
                        active_modes.push((*sink_right, Photon::Vac));
                    } else {
                        active_modes.push((*sink_left, Photon::Vac));
                        active_modes.push((*sink_right, photon));
                    }
                }
            },
            MonteCarloOperator::SPD {source, wp_snowflake, id, time} => {
                result.packets.push(match photon {
                    Photon::Single => WpResult::Success {
                        spd_id: *id,
                        time: *time,
                        wp_snowflake: *wp_snowflake,
                    },
                    Photon::Vac => WpResult::Empty{
                        wp_snowflake: *wp_snowflake,
                    },
                });
                result.ref_cnt += 1;
            },
            MonteCarloOperator::Dump {source} => {
                // Nothing to do
            },
        }
    }
    result
}



// // TODO: come up with a better data structure
// fn find_operator(island: &IslandOfInteraction, mode: ModeIndex) -> ModeIndex {
//     
//     for operator in island.operators.iter() {
//         match operator {
//             Operator::EPPS{..} => {
//                 // we're not interested in EPPS as photons has already been dispatched
//                 continue;
//             },
//             Operator::Single{source_modes, sink_modes, ..} => {
//                 if source_modes[0] != mode {continue;}
//                 return sink_modes[0];
//             },
//             Operator::DualUnivariate{source_modes, sink_modes, ..} => {
//                 if source_modes[0] != mode {continue;}
//                 let rng = rng();
//                 return if rng.random_bool(0.5) {
//                     sink_modes[0]
//                 } else {
//                     sink_modes[1]
//                 };
//             },
//             Operator::SPD{ModeIndex} => {
// 
//             }
// 
//         }
//     }
// }




