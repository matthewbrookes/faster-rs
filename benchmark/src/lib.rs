use std::fs::File;
use std::io::{BufRead, BufReader, Write};

pub fn process_ycsb(input_file: &str, output_file: &str) {
    let input = File::open(input_file).expect("Unable to open input file for reading");
    let mut output = File::create(output_file).expect("Unable to create output file");

    let prefix = "usertable user";

    let reader = BufReader::new(input);
    for line in reader.lines().map(|l| l.unwrap()) {
        let key_start = line.find(prefix).unwrap() + prefix.len();
        let key = &line[key_start..key_start + 16];
        output.write(key.as_bytes()).unwrap();
        output.write(b"\n").unwrap();
    }
}
