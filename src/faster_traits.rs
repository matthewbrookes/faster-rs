extern crate bincode;
extern crate libc;
extern crate libfaster_sys as ffi;

use crate::status;

use bincode::deserialize;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::mpsc::Sender;

pub trait FasterKey: DeserializeOwned + Serialize {}

pub trait FasterValue: DeserializeOwned + Serialize {}

#[inline(always)]
pub unsafe extern "C" fn read_callback<T>(
    sender: *mut libc::c_void,
    value: *const u8,
    length: u64,
    status: u32,
) where
    T: DeserializeOwned,
{
    let boxed_sender = Box::from_raw(sender as *mut Sender<T>);
    let sender = *boxed_sender;
    if status == status::OK.into() {
        let val = deserialize(std::slice::from_raw_parts(value, length as usize)).unwrap();
        // TODO: log error
        let _ = sender.send(val);
    }
}

#[inline(always)]
pub unsafe extern "C" fn read_person_callback(
    sender: *mut libc::c_void,
    person: ffi::person,
    status: u32,
) {
    let boxed_sender = Box::from_raw(sender as *mut Sender<ffi::person>);
    let sender = *boxed_sender;
    if status == status::OK.into() {
        // TODO: log error
        let _ = sender.send(person);
    }
}

#[inline(always)]
pub unsafe extern "C" fn read_auctions_callback(
    sender: *mut libc::c_void,
    buffer: *const u64,
    length: u64,
    status: u32,
) {
    let boxed_sender = Box::from_raw(sender as *mut Sender<&[u64]>);
    let sender = *boxed_sender;
    if status == status::OK.into() {
        // TODO: log error
        let _ = sender.send(std::slice::from_raw_parts(buffer, length as usize));
    }
}

#[inline(always)]
pub unsafe extern "C" fn read_u64_callback(
    sender: *mut libc::c_void,
    value: u64,
    status: u32,
) {
    let boxed_sender = Box::from_raw(sender as *mut Sender<u64>);
    let sender = *boxed_sender;
    if status == status::OK.into() {
        // TODO: log error
        let _ = sender.send(value);
    }
}

#[inline(always)]
pub unsafe extern "C" fn read_u64_pair_callback(
    sender: *mut libc::c_void,
    left: u64,
    right: u64,
    status: u32,
) {
    let boxed_sender = Box::from_raw(sender as *mut Sender<(u64, u64)>);
    let sender = *boxed_sender;
    if status == status::OK.into() {
        // TODO: log error
        let _ = sender.send((left, right));
    }
}

#[inline(always)]
pub unsafe extern "C" fn rmw_callback<T>(
    current: *const u8,
    length_current: u64,
    modification: *mut u8,
    length_modification: u64,
    dst: *mut u8,
) -> u64
where
    T: Serialize + DeserializeOwned + FasterRmw,
{
    let val: T = deserialize(std::slice::from_raw_parts(current, length_current as usize)).unwrap();
    let modif = deserialize(std::slice::from_raw_parts_mut(
        modification,
        length_modification as usize,
    ))
    .unwrap();
    let modified = val.rmw(modif);
    let encoded = bincode::serialize(&modified).unwrap();
    let size = encoded.len();
    if dst != std::ptr::null_mut() {
        encoded.as_ptr().copy_to(dst, size);
    }
    size as u64
}

pub trait FasterRmw: DeserializeOwned + Serialize {
    /// Specify custom Read-Modify-Write logic
    ///
    /// # Example
    /// ```
    /// use faster_rs::{status, FasterKv, FasterRmw};
    /// use serde_derive::{Deserialize, Serialize};
    /// use std::sync::mpsc::Receiver;
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct MyU64 {
    ///     value: u64,
    /// }
    /// impl FasterRmw for MyU64 {
    ///     fn rmw(&self, modification: Self) -> Self {
    ///         MyU64 {
    ///             value: self.value + modification.value,
    ///         }
    ///     }
    /// }
    ///
    /// let store = FasterKv::new_in_memory(32768, 536870912);
    /// let key = 5 as u64;
    /// let value = MyU64 { value: 12 };
    /// let modification = MyU64 { value: 17 };
    /// store.upsert(&key, &value, 1);
    /// store.rmw(&key, &modification, 1);
    /// let (status, recv): (u8, Receiver<MyU64>) = store.read(&key, 1);
    /// assert!(status == status::OK);
    /// assert_eq!(recv.recv().unwrap().value, value.value + modification.value);
    fn rmw(&self, modification: Self) -> Self;
}
