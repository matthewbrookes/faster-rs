extern crate regex;

use faster_kvs::FasterKv;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use regex::Regex;

const kInitCount: u64 = 250000000;
const kTxnCount: u64 = 1000000000;
const kChunkSize: u64 = 3200;
const kRefreshInterval: u64 = 64;
const kCompletePendingInterval: u64 = 1600;

const kNanosPerSecond: u64 = 1000000000;

const kMaxKey: u64 = 268435456;
const kRunSeconds: u64 = 360;
const kCheckpointSeconds: u64 = 30;

pub fn process_ycsb(input_file: &str, output_file: &str) {
    let input = File::open(input_file).expect("Unable to open input file for reading");
    let mut output = File::create(output_file).expect("Unable to create output file");

    let re = Regex::new(r".*usertable user(\d+).*").unwrap();

    let reader = BufReader::new(input);
    for line in reader.lines().map(|l| l.unwrap()) {
        for cap in re.captures_iter(&line) {
            output.write(&cap[1].as_bytes()).unwrap();
            output.write(b"\n").unwrap();
        }
    }
}

pub fn load_file_into_memory(file: &str) -> Vec<u64> {
    let file = File::open(file).expect("Unable to open file for reading keys");
    let mut keys = Vec::new();

    let reader = BufReader::new(file);
    for line in reader.lines().map(|l| l.unwrap()) {
        match line.parse::<u64>() {
            Ok(key) => keys.push(key),
            Err(_e) => eprintln!("Unable to parse {} as u64", line),
        }
    }
    keys
}

pub fn populate_store(store: &Arc<FasterKv>, keys: &Arc<Vec<u64>>, num_threads: u8) {
    let mut threads = vec![];
    let ops = keys.len() as u64;
    let idx = Arc::new(AtomicUsize::new(0));
    for _ in 0..num_threads {
        let store = Arc::clone(store);
        let idx = Arc::clone(&idx);
        let keys = Arc::clone(&keys);
        threads.push(std::thread::spawn(move || {
            let _session = store.start_session();
            let mut i = idx.fetch_add(1, Ordering::SeqCst) as u64;
            while i < ops {
                if i % kRefreshInterval == 0 {
                    store.refresh();
                    if i % kCompletePendingInterval  == 0 {
                        store.complete_pending(false);
                    }
                }
                store.upsert(*keys.get(i as usize).unwrap(), &42, i);
                i = idx.fetch_add(1, Ordering::SeqCst) as u64;
            }
            store.complete_pending(true);
            store.stop_session();
        }));
    }
    for t in threads {
        t.join().expect("Something went wrong in a thread");
    }
}
