extern crate libc;
extern crate bincode;

use crate::status;

use bincode::deserialize;
use serde::Deserialize;
use std::sync::mpsc::Sender;

pub trait FasterValueBinary<'a, T: Deserialize<'a>> {
    extern fn read_callback_binary(sender: *mut libc::c_void, value: *mut u8, length: u64, status: u32) {
        let boxed_sender = unsafe {Box::from_raw(sender as *mut Sender<T>)};
        let sender = *boxed_sender;
        let slice = unsafe {
            deserialize(std::slice::from_raw_parts_mut(value, length as usize)).unwrap()
        };
        if status == status::OK.into() {
            sender.send(slice).unwrap();
        }
    }
}

impl <'a, T> FasterValueBinary<'a, T> for T where T: Deserialize<'a> {}
