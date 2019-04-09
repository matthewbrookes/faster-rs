extern crate clap;

use benchmark::*;
use clap::{App, Arg, SubCommand};
use faster_kvs::FasterKv;
use std::fs::remove_dir;
use std::sync::Arc;

const K_INIT_COUNT: usize = 250000000;
const K_TXN_COUNT: usize = 1000000000;

fn main() {
    let matches = App::new("faster-rs Benchmark")
        .subcommand(
            SubCommand::with_name("process-ycsb")
                .about("Process YCSB file to extract key")
                .arg(
                    Arg::with_name("input")
                        .required(true)
                        .help("Path to input file"),
                )
                .arg(
                    Arg::with_name("output")
                        .required(true)
                        .help("Path to output file"),
                ),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run benchmark")
                .arg(
                    Arg::with_name("num-threads")
                        .short("n")
                        .required(true)
                        .takes_value(true)
                        .display_order(1)
                        .help("Number of threads to use"),
                )
                .arg(
                    Arg::with_name("load")
                        .required(true)
                        .help("Path to YCSB load keys"),
                )
                .arg(
                    Arg::with_name("run")
                        .required(true)
                        .help("Path to YCSB run keys"),
                )
                .arg(
                    Arg::with_name("workload")
                        .required(true)
                        .possible_values(&["read_upsert_50_50", "rmw_100"]),
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("process-ycsb") {
        let input = matches.value_of("input").expect("No input file specified");
        let output = matches
            .value_of("output")
            .expect("No output file specified");
        println!("Processing YCSB workload");
        process_ycsb(input, output);
    } else if let Some(matches) = matches.subcommand_matches("run") {
        let num_threads = matches
            .value_of("num-threads")
            .expect("Number of threads not specified");
        let num_threads: u8 = num_threads
            .parse()
            .expect("num-threads argument must be integer");
        let load_keys = matches
            .value_of("load")
            .expect("File containing load transactions not specified");
        let run_keys = matches
            .value_of("run")
            .expect("File containing run transactions not specified");
        let workload = matches
            .value_of("workload")
            .expect("Workload not specified");
        let op_allocator = match workload {
            "read_upsert_50_50" => read_upsert5050,
            "rmw_100" => rmw_100,
            _ => panic!("Unexpected workload specified. Options are: read_upsert_50_50, rmw_100"),
        };

        let table_size: u64 = 1 << 14;
        let log_size: u64 = 17179869184;
        let dir_path = String::from("benchmark_store");
        let store = Arc::new(FasterKv::new(table_size, log_size, dir_path.clone()).unwrap());
        {
            println!("Populating datastore");
            let keys = Arc::new(load_file_into_memory(load_keys, K_INIT_COUNT));
            populate_store(&store, &keys, num_threads);
        }
        {
            println!("Beginning benchmark");
            let keys = Arc::new(load_file_into_memory(run_keys, K_TXN_COUNT));
            run_benchmark(&store, &keys, num_threads, op_allocator);
        }
        remove_dir(dir_path).expect("Unable to delete store");
    }
}
