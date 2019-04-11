extern crate regex;

use faster_kvs::FasterKv;
use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::prelude::FileExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const K_CHECKPOINT_SECONDS: u64 = 30;
const K_COMPLETE_PENDING_INTERVAL: u64 = 1600;
const K_REFRESH_INTERVAL: u64 = 64;
const K_RUN_TIME: u64 = 360;
const K_CHUNK_SIZE: u64 = 3200;
const K_FILE_CHUNK_SIZE: usize = 131072;
const K_INIT_COUNT: usize = 250000000;
const K_TXN_COUNT: usize = 1000000000;

const K_NANOS_PER_SECOND: usize = 1000000000;

const K_THREAD_STACK_SIZE: usize = 4 * 1024 * 1024;

pub enum Operation {
    Read,
    Upsert,
    Rmw,
}

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

pub fn read_upsert5050(key: usize) -> Operation {
    match key % 2 {
        0 => Operation::Read,
        1 => Operation::Upsert,
        _ => panic!(),
    }
}

pub fn rmw_100(_key: usize) -> Operation {
    Operation::Rmw
}

pub fn load_file_into_memory(file: &str, limit: usize) -> Vec<u64> {
    let file = File::open(file).expect("Unable to open file for reading keys");
    let mut keys = Vec::new();

    let reader = BufReader::new(file);
    for line in reader.lines().map(|l| l.unwrap()) {
        if keys.len() < limit {
            match line.parse::<u64>() {
                Ok(key) => keys.push(key),
                Err(_e) => eprintln!("Unable to parse {} as u64", line),
            }
        } else {
            break;
        }
    }
    keys
}

pub fn load_files(load_file: &str, run_file: &str) -> (Vec<u64>, Vec<u64>) {
    let load_file = File::open(load_file).expect("Unable to open load file");
    let run_file = File::open(run_file).expect("Unable to open run file");

    let mut buffer = [0; K_FILE_CHUNK_SIZE];
    let mut count = 0;
    let mut offset = 0;

    let mut init_keys = Vec::with_capacity(K_INIT_COUNT);

    println!("Loading keys into memory");
    loop {
        let bytes_read = load_file.read_at(&mut buffer, offset).unwrap();
        for i in 0..(bytes_read / 8) {
            let mut num = [0; 8];
            num.copy_from_slice(&buffer[i..i + 8]);
            init_keys.insert(count, u64::from_be_bytes(num));
            count += 1;
        }
        if bytes_read == K_FILE_CHUNK_SIZE {
            offset += K_FILE_CHUNK_SIZE as u64;
        } else {
            break;
        }
    }
    if K_INIT_COUNT != count {
        panic!("Init file load fail!");
    }
    println!("Loaded {} keys", count);

    let mut count = 0;
    let mut offset = 0;

    let mut run_keys = Vec::with_capacity(K_TXN_COUNT);

    println!("Loading txns into memory");
    loop {
        let bytes_read = run_file.read_at(&mut buffer, offset).unwrap();
        for i in 0..(bytes_read / 8) {
            let mut num = [0; 8];
            num.copy_from_slice(&buffer[i..i + 8]);
            run_keys.insert(count, u64::from_be_bytes(num));
            count += 1;
        }
        if bytes_read == K_FILE_CHUNK_SIZE {
            offset += K_FILE_CHUNK_SIZE as u64;
        } else {
            break;
        }
    }
    if K_TXN_COUNT != count {
        panic!("Txn file load fail!");
    }
    println!("Loaded {} txns", count);

    (init_keys, run_keys)
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
                if i % K_REFRESH_INTERVAL == 0 {
                    store.refresh();
                    if i % K_COMPLETE_PENDING_INTERVAL == 0 {
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

pub fn run_benchmark<F: Fn(usize) -> Operation + Send + Copy + 'static>(
    store: &Arc<FasterKv>,
    keys: &Arc<Vec<u64>>,
    num_threads: u8,
    op_allocator: F,
) {
    let idx = Arc::new(AtomicUsize::new(0));
    let mut total_counts = (0, 0, 0, 0);
    let done = Arc::new(AtomicBool::new(false));

    let mut threads = vec![];
    for thread_id in 0..num_threads {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        let idx = Arc::clone(&idx);
        let done = Arc::clone(&done);
        threads.push(
            std::thread::Builder::new()
                .stack_size(K_THREAD_STACK_SIZE)
                .spawn(move || {
                    let mut reads = 0;
                    let mut upserts = 0;
                    let mut rmws = 0;

                    let ops = keys.len();

                    let start = Instant::now();
                    let _session = store.start_session();
                    let mut i = idx.fetch_add(1, Ordering::SeqCst);
                    while i < ops && !done.load(Ordering::Relaxed) {
                        if i as u64 % K_REFRESH_INTERVAL == 0 {
                            store.refresh();
                            if i as u64 % K_COMPLETE_PENDING_INTERVAL == 0 {
                                store.complete_pending(false);
                            }
                        }
                        match op_allocator(i) {
                            Operation::Read => {
                                store.read::<i32>(*keys.get(i).unwrap(), 1);
                                reads += 1;
                            }
                            Operation::Upsert => {
                                store.upsert(*keys.get(i).unwrap(), &42, 1);
                                upserts += 1;
                            }
                            Operation::Rmw => {
                                store.rmw(*keys.get(i).unwrap(), &5, 1);
                                rmws += 1;
                            }
                        }
                        i = idx.fetch_add(1, Ordering::SeqCst);
                    }
                    store.complete_pending(true);
                    store.stop_session();
                    let duration = Instant::now().duration_since(start);

                    println!(
                        "Thread {} completed {} reads, {} upserts and {} rmws in {}ms",
                        thread_id,
                        reads,
                        upserts,
                        rmws,
                        duration.as_millis()
                    );

                    (reads, upserts, rmws, duration.as_nanos())
                })
                .unwrap(),
        )
    }
    let mut last_checkpoint = Instant::now();
    let start = last_checkpoint.clone();
    while idx.load(Ordering::Relaxed) < keys.len() {
        if Instant::now().duration_since(last_checkpoint)
            > Duration::from_secs(K_CHECKPOINT_SECONDS)
        {
            store.checkpoint();
            last_checkpoint = Instant::now();
        }
        if Instant::now().duration_since(start) > Duration::from_secs(K_RUN_TIME) {
            done.store(true, Ordering::Relaxed)
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    for t in threads {
        let (reads, upserts, rmws, duration) = t.join().expect("Something went wrong in a thread");
        total_counts.0 += reads;
        total_counts.1 += upserts;
        total_counts.2 += rmws;
        total_counts.3 += duration;
    }

    println!(
        "Finished benchmark: {} reads, {} writes, {} rmws. {} ops/second/thread",
        total_counts.0,
        total_counts.1,
        total_counts.2,
        (total_counts.0 + total_counts.1 + total_counts.2)
            / (total_counts.3 as usize / K_NANOS_PER_SECOND)
    )
}
