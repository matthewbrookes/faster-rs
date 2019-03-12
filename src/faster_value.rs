extern crate libc;

pub trait FasterValue<T> {
    fn upsert(value: T) -> *mut T {
        let mut boxed_value = Box::from(value);
        Box::leak(boxed_value)
        //&mut *boxed_value
    }
    fn read(ptr: *mut libc::c_void) -> T {
        let boxed_value = unsafe {Box::from_raw(ptr as *mut T)};
        *boxed_value
    }
    fn rmw(value: T, modification: T) -> T;
}

impl FasterValue<u64> for u64 {
    fn rmw(value: u64, modification: u64) -> u64 {
        value + modification
    }
}