extern crate libc;

use crate::status;
use std::sync::mpsc::Sender;
use std::rc::Rc;

pub trait FasterValue<T: Copy> {
    fn upsert(value: T) -> *const T {
        let rc_value = Rc::from(value);
        Rc::into_raw(rc_value)
        //let mut boxed_value = Box::from(value);
        //Box::leak(boxed_value)
        //&mut *boxed_value
    }
    fn read(ptr: *mut libc::c_void) -> T {
        let boxed_value = unsafe {Box::from_raw(ptr as *mut T)};
        *boxed_value
    }
    extern fn read_callback(sender: *mut libc::c_void, value: *mut libc::c_void, status: u32) {
        let boxed_sender = unsafe {Box::from_raw(sender as *mut Sender<T>)};
        let sender = *boxed_sender;
        let boxed_value = unsafe {Rc::from_raw(value as *mut T)};
        let value = *boxed_value;
        if status == status::OK.into() {
            sender.send(value).unwrap();
        }
    }
    fn rmw(value: T, modification: T) -> T;
}

impl FasterValue<u64> for u64 {
    fn rmw(value: u64, modification: u64) -> u64 {
        value + modification
    }
}