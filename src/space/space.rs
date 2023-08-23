use std::sync::{Mutex, RwLock};
use ahash::AHashMap;

pub trait SpaceInterface {
    // region get

    fn get(&self, key: i32, index: usize) -> Option<Vec<u8>>;
    fn get_many(&self, keys: &[String]) -> Vec<&Vec<u8>>;
    fn get_many_map(&self, keys: &[String]) -> AHashMap<String, &Vec<u8>>;

    // endregion

    // region set

    fn set(&self, key: i32, value: Vec<u8>, index: usize);
    /// sets value and returns an old value (or None)
    fn set_and_get(&mut self, key: &str, value: &Vec<u8>) -> Option<&Vec<u8>>;
    fn set_many(&mut self, map: AHashMap<String, &Vec<u8>>);
    /// sets values and returns a map of old values (or None)
    fn set_and_get_many_map(&mut self, map: AHashMap<String, &Vec<u8>>) -> AHashMap<String, &Vec<u8>>;
    fn set_many_array(&mut self, keys: &[String], values: &[&Vec<u8>]);
    /// sets values and returns a vector of old values (or None)
    fn set_and_get_many_array(&mut self, keys: &[String], values: &[&Vec<u8>]) -> Vec<&Vec<u8>>;

    // endregion

    // region delete

    fn delete(&mut self, key: &str);
    fn delete_and_get(&mut self, key: &str) -> Option<&Vec<u8>>;
    fn delete_many(&mut self, keys: &[String]);
    fn delete_and_get_many(&mut self, keys: &[String]) -> Vec<&Vec<u8>>;
    fn delete_many_map(&mut self, keys: &[String]);
    fn delete_and_get_many_map(&mut self, keys: &[String]) -> AHashMap<String, &Vec<u8>>;

    // endregion

    // region other

    fn count(&self) -> usize;
    fn clear(&mut self);

    // endregion

    // region special

        // region numbers
            /// adds the delta to the value. The delta can be positive or negative. Returns the new value.
            fn plus(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>>;
            // multiplies the value by the delta. Returns the new value.
            fn mul(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>>;
            /// divides the value by the delta. Returns the new value.
            fn div(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>>;
            /// exponentiation the value by the delta. Returns the new value.
            fn exponentiation(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>>;

            /// adds the delta (where the delta is a value of this space) to the value. If other_key is None adds 0. Returns the new value.
            fn plus_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>>;
            /// multiplies the value by the delta (where the delta is a value of this space) to the value. If other_key is None adds 0. Returns the new value.
            fn mul_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>>;
            /// divides the value by the delta (where the delta is a value of this space) to the value. If other_key is None adds 0. Returns the new value.
            fn div_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>>;
            /// exponentiation the value by the delta (where the delta is a value of this space) to the value. If other_key is None adds 0. Returns the new value.
            fn exponentiation_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>>;
        // endregion

        // region lists

        /// appends a value to the list. Returns the new length
        fn append(&mut self, key: &str, value: &Vec<u8>) -> usize;
        /// appends many values to the list. Returns the new length
        fn append_many(&mut self, key: &str, values: &[&Vec<u8>]) -> usize;
        /// deletes a value from the list by the index. Returns the new length
        fn delete_from_index(&mut self, key: &str, index: usize) -> usize;
        /// deletes values from the list by the range. Returns the new length
        fn delete_from_range(&mut self, key: &str, start: usize, end: usize)-> usize;
        /// deletes values from the list by the indexes. Returns the new length
        fn delete_by_indexes(&mut self, keys: &str, indexes: &Vec<usize>) -> usize;
        /// gets the last value from the list
        fn get_last(&self, key: &str) -> Option<&Vec<u8>>;
        /// gets a value from the list by index
        fn get_by_index(&self, key: &str, index: usize) -> Option<&Vec<u8>>;
        ///gets values from the list by the range
        fn get_by_range(&self, key: &str, start: usize, end: usize) -> Vec<&Vec<u8>>;
        ///gets values from the list by the indexes
        fn get_by_indexes(&self, key: &str, indexes: &Vec<usize>) -> Vec<&Vec<u8>>;
        /// inserts the value by the index
        fn insert_by_index(&mut self, key: &str, index: usize, value: &Vec<u8>) -> Option<&Vec<u8>>;
        /// inserts many values by the index
        fn insert_many_by_index(&mut self, key: &str, index: usize, values: &Vec<Vec<u8>>) -> usize;
        /// inserts the value by the indexes
        fn insert_by_indexes(&mut self, key: &str, indexes: &Vec<usize>, values: &Vec<Vec<u8>>) -> usize;
        /// inserts many values by the indexes
        fn insert_many_by_indexes(&mut self, key: &str, indexes: &Vec<usize>, values: &Vec<Vec<u8>>) -> usize;
        /// adds the delta to the value. The delta can be positive or negative. Returns the new value
        fn add_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>>;
        /// multiplies the value by the delta. Returns the new value
        fn mul_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>>;
        /// divides the value by the delta. Returns the new value
        fn div_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>>;
        /// exponentiation the value by the delta. Returns the new value
        fn exponentiation_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>>;
        // endregion

        // region objects
            fn get_field(&self, key: &str, field: &str) -> Option<&Vec<u8>>;
            fn get_field_many(&self, key: &str, field: &Vec<&str>) -> Vec<&Vec<u8>>;
            fn set_field(&mut self, key: &str, field: &str, value: &Vec<u8>);
            fn set_field_many(&mut self, key: &str, field: &Vec<&str>, values: &Vec<&Vec<u8>>);
        // endregion

        // region Set
            fn s_set(&mut self, key: &str, set_key: &Vec<u8>);
            fn s_set_many(&mut self, key: &str, set_keys: &Vec<&Vec<u8>>);
            fn s_delete(&mut self, key: &str, set_key: &Vec<u8>);
            fn s_delete_many(&mut self, key: &str, set_keys: &Vec<&Vec<u8>>);
            fn s_contains(&self, key: &str, set_key: &Vec<u8>) -> bool;
            fn s_contains_many(&self, key: &str, set_keys: &Vec<&Vec<u8>>) -> &Vec<u8>;
        // endregion

    // endregion
}

pub type SpaceEngineType = u8;
pub const CACHE: SpaceEngineType = 0;
pub const IN_MEMORY: SpaceEngineType = 1;
pub const ON_DISK: SpaceEngineType = 2;


pub struct Space {
    pub data: Vec<RwLock<AHashMap<i32, Vec<u8>>>>,
    pub engine_type: SpaceEngineType,
    pub size: usize,
}

impl Space {
    pub fn new(engine_type: SpaceEngineType, size: usize) -> Space {
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(RwLock::new(AHashMap::new()));
        }
        Space {
            data,
            size,
            engine_type,
        }
    }
}

impl SpaceInterface for Space {
    fn get(&self, key: i32, index: usize) -> Option<Vec<u8>> {
        match self.data[index % self.size].read().unwrap().get(&key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    fn get_many(&self, keys: &[String]) -> Vec<&Vec<u8>> {
        todo!()
    }

    fn get_many_map(&self, keys: &[String]) -> AHashMap<String, &Vec<u8>> {
        todo!()
    }

    fn set(&self, key: i32, value: Vec<u8>, index: usize) {
        self.data[index % self.size].write().unwrap().insert(key, value);
    }

    fn set_and_get(&mut self, key: &str, value: &Vec<u8>) -> Option<&Vec<u8>> {
        todo!()
    }

    fn set_many(&mut self, map: AHashMap<String, &Vec<u8>>) {
        todo!()
    }

    fn set_and_get_many_map(&mut self, map: AHashMap<String, &Vec<u8>>) -> AHashMap<String, &Vec<u8>> {
        todo!()
    }

    fn set_many_array(&mut self, keys: &[String], values: &[&Vec<u8>]) {
        todo!()
    }

    fn set_and_get_many_array(&mut self, keys: &[String], values: &[&Vec<u8>]) -> Vec<&Vec<u8>> {
        todo!()
    }

    fn delete(&mut self, key: &str) {
        todo!()
    }

    fn delete_and_get(&mut self, key: &str) -> Option<&Vec<u8>> {
        todo!()
    }

    fn delete_many(&mut self, keys: &[String]) {
        todo!()
    }

    fn delete_and_get_many(&mut self, keys: &[String]) -> Vec<&Vec<u8>> {
        todo!()
    }

    fn delete_many_map(&mut self, keys: &[String]) {
        todo!()
    }

    fn delete_and_get_many_map(&mut self, keys: &[String]) -> AHashMap<String, &Vec<u8>> {
        todo!()
    }

    fn count(&self) -> usize {
        todo!()
    }

    fn clear(&mut self) {
        todo!()
    }

    fn plus(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>> {
        todo!()
    }

    fn mul(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>> {
        todo!()
    }

    fn div(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>> {
        todo!()
    }

    fn exponentiation(&mut self, key: &str, delta: i64) -> Option<&Vec<u8>> {
        todo!()
    }

    fn plus_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>> {
        todo!()
    }

    fn mul_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>> {
        todo!()
    }

    fn div_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>> {
        todo!()
    }

    fn exponentiation_other_key(&mut self, key: &str, other_key: &str) -> Option<&Vec<u8>> {
        todo!()
    }

    fn append(&mut self, key: &str, value: &Vec<u8>) -> usize {
        todo!()
    }

    fn append_many(&mut self, key: &str, values: &[&Vec<u8>]) -> usize {
        todo!()
    }

    fn delete_from_index(&mut self, key: &str, index: usize) -> usize {
        todo!()
    }

    fn delete_from_range(&mut self, key: &str, start: usize, end: usize) -> usize {
        todo!()
    }

    fn delete_by_indexes(&mut self, keys: &str, indexes: &Vec<usize>) -> usize {
        todo!()
    }

    fn get_last(&self, key: &str) -> Option<&Vec<u8>> {
        todo!()
    }

    fn get_by_index(&self, key: &str, index: usize) -> Option<&Vec<u8>> {
        todo!()
    }

    fn get_by_range(&self, key: &str, start: usize, end: usize) -> Vec<&Vec<u8>> {
        todo!()
    }

    fn get_by_indexes(&self, key: &str, indexes: &Vec<usize>) -> Vec<&Vec<u8>> {
        todo!()
    }

    fn insert_by_index(&mut self, key: &str, index: usize, value: &Vec<u8>) -> Option<&Vec<u8>> {
        todo!()
    }

    fn insert_many_by_index(&mut self, key: &str, index: usize, values: &Vec<Vec<u8>>) -> usize {
        todo!()
    }

    fn insert_by_indexes(&mut self, key: &str, indexes: &Vec<usize>, values: &Vec<Vec<u8>>) -> usize {
        todo!()
    }

    fn insert_many_by_indexes(&mut self, key: &str, indexes: &Vec<usize>, values: &Vec<Vec<u8>>) -> usize {
        todo!()
    }

    fn add_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>> {
        todo!()
    }

    fn mul_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>> {
        todo!()
    }

    fn div_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>> {
        todo!()
    }

    fn exponentiation_by_index(&mut self, key: &str, index: usize, delta: &Vec<u8>) -> Option<&Vec<u8>> {
        todo!()
    }

    fn get_field(&self, key: &str, field: &str) -> Option<&Vec<u8>> {
        todo!()
    }

    fn get_field_many(&self, key: &str, field: &Vec<&str>) -> Vec<&Vec<u8>> {
        todo!()
    }

    fn set_field(&mut self, key: &str, field: &str, value: &Vec<u8>) {
        todo!()
    }

    fn set_field_many(&mut self, key: &str, field: &Vec<&str>, values: &Vec<&Vec<u8>>) {
        todo!()
    }

    fn s_set(&mut self, key: &str, set_key: &Vec<u8>) {
        todo!()
    }

    fn s_set_many(&mut self, key: &str, set_keys: &Vec<&Vec<u8>>) {
        todo!()
    }

    fn s_delete(&mut self, key: &str, set_key: &Vec<u8>) {
        todo!()
    }

    fn s_delete_many(&mut self, key: &str, set_keys: &Vec<&Vec<u8>>) {
        todo!()
    }

    fn s_contains(&self, key: &str, set_key: &Vec<u8>) -> bool {
        todo!()
    }

    fn s_contains_many(&self, key: &str, set_keys: &Vec<&Vec<u8>>) -> &Vec<u8> {
        todo!()
    }
}