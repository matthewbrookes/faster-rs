# Experimental FASTER wrapper for Rust

Includes experimental C interface for FASTER. It currently assumes the KEY type is u64 however the VALUE type supports arbitrary serialisable structs. This wrapper is only focusing on Linux support.


It is probably a good idea to make sure you can compile the C++ version before you start playing around with this wrapper.

*Make sure you clone the submodules as well*, this is best done by cloning with `git clone --recurse-submodules`.


## A basic example

The following example shows the creation of a FASTER Key-Value Store and basic operations on `u64` values.

Try it out by running `cargo run --example basic`.

```rust,no_run
extern crate faster_kvs;

use faster_kvs::{FasterKv, status};
use std::sync::mpsc::Receiver;

fn main() {
    const TABLE_SIZE: u64  = 1 << 14;
    const LOG_SIZE: u64 = 17179869184;

    // Create a Key-Value Store
    if let Ok(store) = FasterKv::new(TABLE_SIZE, LOG_SIZE, String::from("example_basic_storage")) {
        let key0: u64 = 1;
        let value0: u64 = 1000;
        let modification: u64 = 5;

        // Upsert
        for i in 0..1000 {
            let upsert = store.upsert(key0 + i, &(value0 + i), i);
            assert!(upsert == status::OK || upsert == status::PENDING);
        }

        // Read-Modify-Write
        for i in 0..1000 {
            let rmw = store.rmw(key0 + i, &(5 as u64), i + 1000);
            assert!(rmw == status::OK || rmw == status::PENDING);
        }

        assert!(store.size() > 0);

        // Read
        for i in 0..1000 {
            // Note: need to provide type annotation for the Receiver
            let (read, recv): (u8, Receiver<u64>) = store.read(key0 + i, i);
            assert!(read == status::OK || read == status::PENDING);
            let val = recv.recv().unwrap();
            assert_eq!(val, value0 + i + modification);
            println!("Key: {}, Value: {}", key0 + i, val);
        }

        // Clear used storage
        match store.clean_storage() {
            Ok(()) => {},
            Err(_err) => panic!("Unable to clear FASTER directory"),
        }
    } else {
        panic!("Unable to create FASTER directory");
    }
}
```

## Using custom values
`struct`s that can be (de)serialised using [serde](https://crates.rs/crates/serde) are supported as values. In order to use such a `struct`, it is necessary to derive the implementations of `Serializable` and `Deserializable` from `serde-derive`. It is also necessary to implement the `FasterValue` trait which exposes an `rmw()` function. This function can be used to implement custom logic for Read-Modify-Write operations or simply left with an `unimplemented!()` macro. In the latter case, any attempt to invoke a RMW operation will cause a panic.

The following example shows a basic struct being used as a value. Try it out by running `cargo run --example custom_values`.

```rust,no_run
extern crate faster_kvs;
extern crate serde_derive;

use faster_kvs::{FasterKv, FasterValue,status};
use serde_derive::{Deserialize, Serialize};
use std::sync::mpsc::Receiver;

// Note: Debug annotation is just for printing later
#[derive(Serialize, Deserialize, Debug)]
struct MyValue {
    foo: String,
    bar: String,
}

impl FasterValue<'_, MyValue> for MyValue {
    fn rmw(&self, _modification: MyValue) -> MyValue {
        unimplemented!()
    }
}

fn main() {
    const TABLE_SIZE: u64  = 1 << 14;
    const LOG_SIZE: u64 = 17179869184;

    // Create a Key-Value Store
    if let Ok(store) = FasterKv::new(TABLE_SIZE, LOG_SIZE, String::from("example_custom_values_storage")) {
        let key: u64 = 1;
        let value = MyValue { foo: String::from("Hello"), bar: String::from("World") };

        // Upsert
        let upsert = store.upsert(key, &value, 1);
        assert!(upsert == status::OK || upsert == status::PENDING);

        assert!(store.size() > 0);

        // Note: need to provide type annotation for the Receiver
        let (read, recv): (u8, Receiver<MyValue>) = store.read(key, 1);
        assert!(read == status::OK || read == status::PENDING);
        let val = recv.recv().unwrap();
        println!("Key: {}, Value: {:?}", key, val);

        // Clear used storage
        match store.clean_storage() {
            Ok(()) => {},
            Err(_err) => panic!("Unable to clear FASTER directory"),
        }
    } else {
        panic!("Unable to create FASTER directory");
    }
}
```

## Out-of-the-box implementations of `FasterValue`
Several types already implement `FasterValue` along with providing Read-Modify-Write logic. The implementations can be found in `src/impls.rs` but their RMW logic is summarised here:
* Numeric types use addition
* Bools and Chars replace old value for new value
* Strings and Vectors append new values (use an `upsert` to replace entire value)

## Checkpoint and Recovery
FASTER's fault tolerance is provided by [Concurrent Prefix Recovery](https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf) (CPR). It provides the following semantics:
 > If operation X is persisted, then all operations before X in the input operation sequence are persisted as well (and none after).

The `Read`, `Upsert` and `RMW` operations all require a monotonic serial number to identify the specific operation in the sequence for CPR. The most recently persisted serial number is returned by the `continue_session()` function and allows reasoning about which operations were (not) persisted. It is also the operation sequence number from which the thread should continue to provide operations after recovery. If persistence is not important, the serial number can safely be set to `1` for all operations (as is done in the examples above).

Persisting operations is done using the `checkpoint()` function. It is also important to periodically call the `refresh()` function as it is the mechanism threads use to report forward progress to the system.

A good demonstration of checkpointing/recovery can be found in `examples/single_threaded_recovery.rs`. Try it out for yourself!
```bash
$ cargo run --example single_threaded_recovery -- populate
$ cargo run --example single_threaded_recovery -- recover <checkpoint-token>
```
# Things to fix

- [x] Fix so you can actually return the values from read
- [ ] Experiment with #repr(C) structs for values rather than u64
- [ ] Look into threading and async callbacks into Rust
- [ ] Finish off the rest off the operations in the C interface
- [ ] Compare performance to C++ version
