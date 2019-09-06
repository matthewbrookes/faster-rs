extern crate faster_rs;
extern crate tempfile;

use faster_rs::{status, FasterKv};
use std::collections::HashSet;
use std::sync::mpsc::Receiver;
use tempfile::TempDir;
use std::ffi::CStr;

const TABLE_SIZE: u64 = 1 << 14;
const LOG_SIZE: u64 = 17179869184;

#[test]
fn insert_read_person() {
    let tmp_dir = TempDir::new().unwrap();
    let dir_path = tmp_dir.path().to_string_lossy().into_owned();
    let store = FasterKv::new_person_store(TABLE_SIZE, LOG_SIZE, dir_path).unwrap();
    let key: u64 = 1;
    {
        let upsert = store.upsert_person(key, "name", "city", "state", 1);
        assert!((upsert == status::OK || upsert == status::PENDING) == true);
    }

    let (read, recv) = store.read_person(key, 1);
    assert_eq!(read, status::OK);
    let person = recv.recv().unwrap();
    unsafe {
        assert_eq!(CStr::from_ptr(person.name).to_str(), Ok("name"));
        assert_eq!(CStr::from_ptr(person.city).to_str(), Ok("city"));
        assert_eq!(CStr::from_ptr(person.state).to_str(), Ok("state"));
    }

    assert!(store.size() > 0);
}

#[test]
fn rmw_read_auction() {
    let tmp_dir = TempDir::new().unwrap();
    let dir_path = tmp_dir.path().to_string_lossy().into_owned();
    let store = FasterKv::new_auctions_store(TABLE_SIZE, LOG_SIZE, dir_path).unwrap();
    let key: u64 = 1;
    for i in 0..100 {
        store.rmw_auction(1, i, 1);
    }
    let (res, recv) = store.read_auctions(key, 1);
    assert_eq!(res, status::OK);

    let mut expected = 0;
    for actual in recv.recv().unwrap() {
        assert_eq!(expected, *actual);
        expected += 1;
    }

    let (res, recv) = store.read_auctions(key + 1, 1);
    assert_eq!(res, status::NOT_FOUND);
    assert!(recv.recv().is_err());
}
