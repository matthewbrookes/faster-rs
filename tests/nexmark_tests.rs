extern crate faster_rs;
extern crate tempfile;

use faster_rs::{status, FasterKv, FasterIterator};
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

    {
        let (read, recv) = store.read_person(key, 1);
        assert_eq!(read, status::OK);
        let person = recv.recv().unwrap();
        unsafe {
            assert_eq!(CStr::from_ptr(person.name).to_str(), Ok("name"));
            assert_eq!(CStr::from_ptr(person.city).to_str(), Ok("city"));
            assert_eq!(CStr::from_ptr(person.state).to_str(), Ok("state"));
        }
    }

    {
        let (read, recv) = store.read_person(key, 1);
        assert_eq!(read, status::OK);
        let person = recv.recv().unwrap();
        unsafe {
            assert_eq!(CStr::from_ptr(person.name).to_str(), Ok("name"));
            assert_eq!(CStr::from_ptr(person.city).to_str(), Ok("city"));
            assert_eq!(CStr::from_ptr(person.state).to_str(), Ok("state"));
        }
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
    store.rmw_auctions(1, (100..200).collect(), 1);
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

#[test]
fn insert_auctions() {
    let tmp_dir = TempDir::new().unwrap();
    let dir_path = tmp_dir.path().to_string_lossy().into_owned();
    let store = FasterKv::new_auctions_store(TABLE_SIZE, LOG_SIZE, dir_path).unwrap();
    let key: u64 = 1;
    let auctions: Vec<u64> = (0..100).collect();
    store.upsert_auctions(key, auctions, 1);

    let (res, recv) = store.read_auctions(key, 1);
    assert_eq!(res, status::OK);

    let mut expected = 0;
    for actual in recv.recv().unwrap() {
        assert_eq!(expected, *actual);
        expected += 1;
    }
}

#[test]
fn u64_operations() {
    let tmp_dir = TempDir::new().unwrap();
    let dir_path = tmp_dir.path().to_string_lossy().into_owned();
    let store = FasterKv::new_u64_store(TABLE_SIZE, LOG_SIZE, dir_path).unwrap();

    for i in 0..200 {
        store.upsert_u64(i, 42, 1);
    }

    for i in 0..100 {
        store.rmw_u64(i, 42, 1);
    }

    for i in 80..120 {
        store.delete_u64(i, 1);
    }

    for i in 0..80 {
        let (res, recv) = store.read_u64(i, 1);
        assert_eq!(res, status::OK);
        assert_eq!(recv.recv().unwrap(), 84);
    }

    for i in 80..120 {
        let (res, recv) = store.read_u64(i, 1);
        assert_eq!(res, status::NOT_FOUND);
        assert!(recv.recv().is_err());
    }

    for i in 120..200 {
        let (res, recv) = store.read_u64(i, 1);
        assert_eq!(res, status::OK);
        assert_eq!(recv.recv().unwrap(), 42);
    }

    let mut expected_key = 0;
    let iterator = store.get_iterator_u64();
    while let Some(record) = iterator.get_next() {
        assert_eq!(expected_key, record.key.unwrap());
        if expected_key < 100 {
            assert_eq!(84, record.value.unwrap());
        }
        if expected_key >= 100 && expected_key < 200 {
            assert_eq!(42, record.value.unwrap());
        }
        if expected_key == 79 {
            expected_key = 120;
        } else {
            expected_key += 1;
        }
    }
}

#[test]
fn pair_u64_operations() {

    let tmp_dir = TempDir::new().unwrap();
    let dir_path = tmp_dir.path().to_string_lossy().into_owned();
    let store = FasterKv::new_u64_pair_store(TABLE_SIZE, LOG_SIZE, dir_path).unwrap();

    store.upsert_u64_pair(1, (100, 200), 1);
    store.rmw_u64_pair(1, (300, 800), 1);

    let (res, recv) = store.read_u64_pair(1, 1);
    assert_eq!(res, status::OK);
    let (left, right) = recv.recv().unwrap();
    assert_eq!((*left, *right), (400, 1000));
}
