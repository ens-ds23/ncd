use std::{collections::HashMap, io};

use crate::write::{NCDValueSource};

pub struct NCDHashMapValueSourceIterator<'a> {
    index: usize,
    keys: Vec<&'a Vec<u8>>,
    source: &'a NCDHashMapValueSource
}

impl<'a> NCDHashMapValueSourceIterator<'a> {
    fn new(source: &'a NCDHashMapValueSource) -> NCDHashMapValueSourceIterator<'a> {
        let mut keys : Vec<_> = source.0.keys().collect();
        keys.sort_by_cached_key(|x| x.len());
        NCDHashMapValueSourceIterator {
            index: 0,
            keys,
            source
        }
    }
}

impl<'a> Iterator for NCDHashMapValueSourceIterator<'a> {
    type Item = io::Result<(Vec<u8>,Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.source.0.len() {
            return None;
        }
        self.index += 1;
        let key = self.keys[self.index-1];
        Some(Ok((key.to_vec(),self.source.0.get(key).unwrap().to_vec())))
    }
}

pub struct NCDHashMapValueSource(HashMap<Vec<u8>,Vec<u8>>);

impl NCDHashMapValueSource {
    pub fn new(hashmap: HashMap<Vec<u8>,Vec<u8>>) -> NCDHashMapValueSource {
        NCDHashMapValueSource(hashmap)
    }
}

impl NCDValueSource for NCDHashMapValueSource {
    fn iter<'a>(&'a self) -> io::Result<Box<dyn Iterator<Item=io::Result<(Vec<u8>,Vec<u8>)>> + 'a>> {
        Ok(Box::new(NCDHashMapValueSourceIterator::new(&self)) as Box<dyn Iterator<Item=io::Result<(Vec<u8>,Vec<u8>)>>>)
    }
}
