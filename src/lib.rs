extern crate bincode;
extern crate libc;
extern crate libfaster_sys as ffi;

mod faster_error;
mod faster_traits;
mod impls;
pub mod status;
mod util;

pub use crate::faster_error::FasterError;
use crate::faster_traits::{read_callback, read_person_callback, read_auctions_callback, rmw_callback};
pub use crate::faster_traits::{FasterKey, FasterRmw, FasterValue};
use crate::util::*;

pub use ffi::person as Person;

use std::ffi::CStr;
use std::ffi::CString;
use std::fs;
use std::io;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::os::raw::c_void;
use std::os::raw::c_char;

pub struct FasterKv {
    faster_t: *mut ffi::faster_t,
    storage_dir: Option<String>,
}

pub struct FasterIteratorRecord<K, V> {
    pub status: bool,
    pub key: Option<K>,
    pub value: Option<V>,
    result: *mut ffi::faster_iterator_result,
}

impl<K, V> Drop for FasterIteratorRecord<K, V> {
    fn drop(&mut self) {
        unsafe {
            ffi::faster_iterator_result_destroy(self.result);
        }
    }
}

pub struct FasterIterator {
    iterator: *mut c_void,
    record: *mut c_void,
}

impl Drop for FasterIterator {
    fn drop(&mut self) {
        unsafe {
            ffi::faster_scan_in_memory_destroy(self.iterator);
            ffi::faster_scan_in_memory_record_destroy(self.record);
        }
    }
}

impl FasterIterator {
    pub fn get_next<K, V>(&self) -> FasterIteratorRecord<K, V>
    where
        K: FasterKey,
        V: FasterValue
    {
        let result = unsafe {
            ffi::faster_iterator_get_next(self.iterator, self.record)
        };
        let status = unsafe {(*result).status};
        if !status {
            return FasterIteratorRecord {
                status,
                key: None,
                value: None,
                result
            }
        }
        let key = Some(bincode::deserialize(unsafe { std::slice::from_raw_parts((*result).key, (*result).key_length as usize) }).unwrap());
        let value = Some(bincode::deserialize(unsafe { std::slice::from_raw_parts((*result).value, (*result).value_length as usize) }).unwrap());
        FasterIteratorRecord {
            status,
            key,
            value,
            result
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn deallocate_vec(vec: *mut u8, length: u64) {
    drop(Vec::from_raw_parts(vec, length as usize, length as usize));
}

#[no_mangle]
pub unsafe extern "C" fn deallocate_string(str: *mut c_char) {
    CString::from_raw(str);
}

impl FasterKv {
    pub fn new(
        table_size: u64,
        log_size: u64,
        storage_name: String,
    ) -> Result<FasterKv, io::Error> {
        let saved_dir = storage_name.clone();
        let storage_str = CString::new(storage_name).unwrap();
        let ptr_raw = storage_str.into_raw();
        let faster_t = unsafe {
            let ft = ffi::faster_open_with_disk(table_size, log_size, ptr_raw);
            let _ = CString::from_raw(ptr_raw); // retake pointer to free mem
            ft
        };
        Ok(FasterKv {
            faster_t: faster_t,
            storage_dir: Some(saved_dir),
        })
    }

    pub fn new_in_memory(table_size: u64, log_size: u64) -> FasterKv {
        let faster_t = unsafe { ffi::faster_open(table_size, log_size) };
        FasterKv {
            faster_t,
            storage_dir: None,
        }
    }

    pub fn new_person_store(
        table_size: u64,
        log_size: u64,
        storage_name: String,
    ) -> Result<FasterKv, io::Error> {
        let saved_dir = storage_name.clone();
        let storage_str = CString::new(storage_name).unwrap();
        let ptr_raw = storage_str.into_raw();
        let faster_t = unsafe {
            let ft = ffi::faster_open_with_disk_people(table_size, log_size, ptr_raw);
            let _ = CString::from_raw(ptr_raw); // retake pointer to free mem
            ft
        };
        Ok(FasterKv {
            faster_t: faster_t,
            storage_dir: Some(saved_dir),
        })
    }

    pub fn new_auctions_store(
        table_size: u64,
        log_size: u64,
        storage_name: String,
    ) -> Result<FasterKv, io::Error> {
        let saved_dir = storage_name.clone();
        let storage_str = CString::new(storage_name).unwrap();
        let ptr_raw = storage_str.into_raw();
        let faster_t = unsafe {
            let ft = ffi::faster_open_with_disk_auctions(table_size, log_size, ptr_raw);
            let _ = CString::from_raw(ptr_raw); // retake pointer to free mem
            ft
        };
        Ok(FasterKv {
            faster_t: faster_t,
            storage_dir: Some(saved_dir),
        })
    }

    pub fn upsert<K, V>(&self, key: &K, value: &V, monotonic_serial_number: u64) -> u8
    where
        K: FasterKey,
        V: FasterValue,
    {
        let mut encoded_key = bincode::serialize(key).unwrap();
        let encoded_key_length = encoded_key.len();
        let encoded_key_ptr = encoded_key.as_mut_ptr();
        let mut encoded_value = bincode::serialize(value).unwrap();
        let encoded_value_length = encoded_value.len();
        let encoded_value_ptr = encoded_value.as_mut_ptr();
        std::mem::forget(encoded_key);
        std::mem::forget(encoded_value);
        unsafe {
            ffi::faster_upsert(
                self.faster_t,
                encoded_key_ptr,
                encoded_key_length as u64,
                encoded_value_ptr,
                encoded_value_length as u64,
                monotonic_serial_number,
            )
        }
    }

    pub fn upsert_person(&self, id: u64, name: &str, city: &str, state: &str, monotonic_serial_number: u64) -> u8 {
        let person = ffi::person {
            name: CString::new(name).unwrap().into_raw(),
            name_length: name.len() + 1,
            city: CString::new(city).unwrap().into_raw(),
            city_length: city.len() + 1,
            state: CString::new(state).unwrap().into_raw(),
            state_length: state.len() + 1,
        };
        unsafe {
            ffi::faster_upsert_person(
                self.faster_t,
                id,
                person,
                monotonic_serial_number
            )
        }
    }

    pub fn read<K, V>(&self, key: &K, monotonic_serial_number: u64) -> (u8, Receiver<V>)
    where
        K: FasterKey,
        V: FasterValue,
    {
        let mut encoded_key = bincode::serialize(key).unwrap();
        let encoded_key_length = encoded_key.len();
        let encoded_key_ptr = encoded_key.as_mut_ptr();
        let (sender, receiver) = channel();
        let sender_ptr: *mut Sender<V> = Box::into_raw(Box::new(sender));
        std::mem::forget(encoded_key);
        let status = unsafe {
            ffi::faster_read(
                self.faster_t,
                encoded_key_ptr,
                encoded_key_length as u64,
                monotonic_serial_number,
                Some(read_callback::<V>),
                sender_ptr as *mut libc::c_void,
            )
        };
        (status, receiver)
    }

    pub fn read_person(&self, id: u64, monotonic_serial_number: u64) -> (u8, Receiver<Person>) {
        let (sender, receiver) = channel();
        let sender_ptr: *mut Sender<ffi::person> = Box::into_raw(Box::new(sender));
        let status = unsafe {
            ffi::faster_read_person(
                self.faster_t,
                id,
                monotonic_serial_number,
                Some(read_person_callback),
                sender_ptr as *mut libc::c_void,
            )
        };
        (status, receiver)
    }

    pub fn read_auctions(&self, id: u64, monotonic_serial_number: u64) -> (u8, Receiver<&[u64]>) {
        let (sender, receiver) = channel();
        let sender_ptr: *mut Sender<&[u64]> = Box::into_raw(Box::new(sender));
        let status = unsafe {
            ffi::faster_read_auctions(
                self.faster_t,
                id,
                monotonic_serial_number,
                Some(read_auctions_callback),
                sender_ptr as *mut libc::c_void,
            )
        };
        (status, receiver)
    }

    pub fn rmw<K, V>(&self, key: &K, value: &V, monotonic_serial_number: u64) -> u8
    where
        K: FasterKey,
        V: FasterRmw,
    {
        let mut encoded_key = bincode::serialize(key).unwrap();
        let encoded_key_length = encoded_key.len();
        let encoded_key_ptr = encoded_key.as_mut_ptr();
        let mut encoded_value = bincode::serialize(value).unwrap();
        let encoded_value_length = encoded_value.len();
        let encoded_value_ptr = encoded_value.as_mut_ptr();
        std::mem::forget(encoded_key);
        std::mem::forget(encoded_value);
        unsafe {
            ffi::faster_rmw(
                self.faster_t,
                encoded_key_ptr,
                encoded_key_length as u64,
                encoded_value_ptr,
                encoded_value_length as u64,
                monotonic_serial_number,
                Some(rmw_callback::<V>),
            )
        }
    }

    pub fn delete<K>(&self, key: &K, monotonic_serial_number: u64) -> u8
    where
        K: FasterKey
    {
        let mut encoded_key = bincode::serialize(key).unwrap();
        let encoded_key_length = encoded_key.len();
        let encoded_key_ptr = encoded_key.as_mut_ptr();
        std::mem::forget(encoded_key);
        unsafe {
            ffi::faster_delete(
                self.faster_t,
                encoded_key_ptr,
                encoded_key_length as u64,
                monotonic_serial_number
            )
        }
    }
    pub fn rmw_auction(&self, key: u64, auction: u64, monotonic_serial_number: u64) -> u8 {
        unsafe {
            ffi::faster_rmw_auction(
                self.faster_t,
                key,
                auction,
                monotonic_serial_number
            )
        }
    }

    pub fn get_iterator(&self) -> FasterIterator {
        let iterator = unsafe {
            ffi::faster_scan_in_memory_init(self.faster_t)
        };
        let record = unsafe {
            ffi::faster_scan_in_memory_record_init()
        };
        FasterIterator {
            iterator,
            record,
        }
    }

    pub fn size(&self) -> u64 {
        unsafe { ffi::faster_size(self.faster_t) }
    }

    pub fn checkpoint(&self) -> Result<CheckPoint, FasterError> {
        if self.storage_dir.is_none() {
            return Err(FasterError::InvalidType);
        }

        let result = unsafe { ffi::faster_checkpoint(self.faster_t) };
        match result.is_null() {
            true => Err(FasterError::CheckpointError),
            false => {
                let boxed = unsafe { Box::from_raw(result) }; // makes sure memory is dropped
                let token_str =
                    unsafe { CStr::from_ptr((*boxed).token).to_str().unwrap().to_owned() };

                let checkpoint = CheckPoint {
                    checked: (*boxed).checked,
                    token: token_str,
                };
                Ok(checkpoint)
            }
        }
    }

    pub fn checkpoint_index(&self) -> Result<CheckPoint, FasterError> {
        if self.storage_dir.is_none() {
            return Err(FasterError::InvalidType);
        }

        let result = unsafe { ffi::faster_checkpoint(self.faster_t) };
        match result.is_null() {
            true => Err(FasterError::CheckpointError),
            false => {
                let boxed = unsafe { Box::from_raw(result) }; // makes sure memory is dropped
                let token_str =
                    unsafe { CStr::from_ptr((*boxed).token).to_str().unwrap().to_owned() };

                let checkpoint = CheckPoint {
                    checked: (*boxed).checked,
                    token: token_str,
                };
                Ok(checkpoint)
            }
        }
    }

    pub fn checkpoint_hybrid_log(&self) -> Result<CheckPoint, FasterError> {
        if self.storage_dir.is_none() {
            return Err(FasterError::InvalidType);
        }

        let result = unsafe { ffi::faster_checkpoint(self.faster_t) };
        match result.is_null() {
            true => Err(FasterError::CheckpointError),
            false => {
                let boxed = unsafe { Box::from_raw(result) }; // makes sure memory is dropped
                let token_str =
                    unsafe { CStr::from_ptr((*boxed).token).to_str().unwrap().to_owned() };

                let checkpoint = CheckPoint {
                    checked: (*boxed).checked,
                    token: token_str,
                };
                Ok(checkpoint)
            }
        }
    }

    pub fn recover(
        &self,
        index_token: String,
        hybrid_log_token: String,
    ) -> Result<Recover, FasterError> {
        if self.storage_dir.is_none() {
            return Err(FasterError::InvalidType);
        }
        let index_token_c = CString::new(index_token).unwrap();
        let index_token_ptr = index_token_c.into_raw();

        let hybrid_token_c = CString::new(hybrid_log_token).unwrap();
        let hybrid_token_ptr = hybrid_token_c.into_raw();

        let recover_result = unsafe {
            let rec = ffi::faster_recover(self.faster_t, index_token_ptr, hybrid_token_ptr);
            let _ = CString::from_raw(index_token_ptr);
            let _ = CString::from_raw(hybrid_token_ptr);
            rec
        };

        match recover_result.is_null() {
            true => Err(FasterError::RecoveryError),
            false => {
                let boxed = unsafe { Box::from_raw(recover_result) }; // makes sure mem is freed
                let sessions_count = (*boxed).session_ids_count;
                let mut session_ids_vec: Vec<String> = Vec::new();
                for i in 0..sessions_count {
                    let id = unsafe {
                        CStr::from_ptr(((*boxed).session_ids).offset(37 * i as isize))
                            .to_str()
                            .unwrap()
                            .to_owned()
                    };
                    session_ids_vec.push(id);
                }
                let recover = Recover {
                    status: (*boxed).status,
                    version: (*boxed).version,
                    session_ids: session_ids_vec,
                };
                Ok(recover)
            }
        }
    }

    pub fn complete_pending(&self, b: bool) -> () {
        unsafe { ffi::faster_complete_pending(self.faster_t, b) }
    }

    pub fn start_session(&self) -> String {
        unsafe {
            let c_guid = ffi::faster_start_session(self.faster_t);
            let rust_str = CStr::from_ptr(c_guid).to_str().unwrap().to_owned();
            rust_str
        }
    }

    pub fn continue_session(&self, token: String) -> u64 {
        let token_str = CString::new(token).unwrap();
        let token_ptr = token_str.into_raw();
        unsafe {
            let result = ffi::faster_continue_session(self.faster_t, token_ptr);
            let _ = CString::from_raw(token_ptr);
            result
        }
    }

    pub fn stop_session(&self) -> () {
        unsafe { ffi::faster_stop_session(self.faster_t) }
    }

    pub fn refresh(&self) -> () {
        unsafe {
            ffi::faster_refresh_session(self.faster_t);
        }
    }

    pub fn dump_distribution(&self) -> () {
        unsafe {
            ffi::faster_dump_distribution(self.faster_t);
        }
    }

    pub fn grow_index(&self) -> bool {
        unsafe { ffi::faster_grow_index(self.faster_t) }
    }

    // Warning: Calling this will remove the stored data
    pub fn clean_storage(&self) -> Result<(), FasterError> {
        match &self.storage_dir {
            None => Err(FasterError::InvalidType),
            Some(dir) => {
                fs::remove_dir_all(dir)?;
                Ok(())
            }
        }
    }

    fn destroy(&self) -> () {
        unsafe {
            ffi::faster_destroy(self.faster_t);
        }
    }
}

// In order to make sure we release the resources the C interface has allocated for the store
impl Drop for FasterKv {
    fn drop(&mut self) {
        self.destroy();
    }
}

unsafe impl Send for FasterKv {}
unsafe impl Sync for FasterKv {}
