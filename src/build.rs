use std::{path::{Path, PathBuf}, time::{SystemTime, UNIX_EPOCH}};

use crate::{header::{HEADER_SIZE, NCDHeader}, util::{NCDError, wrap_io_error}, write::{ NCDValueSource, NCDWriteAttempt }};

const KB : u32 = 1024;

#[derive(Clone)]
pub struct NCDBuildConfig {
    target_page_size: u32,
    target_load_factor: f64,
    heap_wiggle_room: f64,
    min_entries_per_page: u64,
    external_trheshold: f64,
    rebuild_page_factor: f64,
    force_header_size: Option<u32>
}

impl NCDBuildConfig {
    pub fn new() -> NCDBuildConfig {
        NCDBuildConfig {
            target_page_size: 32*KB,
            target_load_factor: 0.5,
            heap_wiggle_room: 1.25,
            min_entries_per_page: 100,
            external_trheshold: 0.1,
            rebuild_page_factor: 1.2,
            force_header_size: None
        }
    }

    chain!(target_page_size,get_target_page_size,u32,NCDBuildConfig);
    chain!(target_load_factor,get_target_load_factor,f64,NCDBuildConfig);
    chain!(heap_wiggle_room,get_heap_wiggle_room,f64,NCDBuildConfig);
    chain!(min_entries_per_page,get_min_entries_per_page,u64,NCDBuildConfig);
    chain!(external_trheshold,get_external_trheshold,f64,NCDBuildConfig);
    chain!(rebuild_page_factor,get_rebuild_page_factor,f64,NCDBuildConfig);
    chain!(force_header_size,get_force_header_size,Option<u32>,NCDBuildConfig);
}

/* Parameters:
 * S = target page size
 * f = target load factor
 * k = table pointer length (=2 if S<65536, otherwise 4)
 * N = number of keys
 * L = toatal data length
 * w = heap wiggle-room factor (to avoid heap full due to variance)
 * 
 * Calculate:
 * p = number of pages
 *
 * Derived:
 * N/p          occupied table entries per page
 * N/pf         available table entries per page
 * kN/pf        table length in bytes
 * wL/p         heap required per page
 * kN/pf + wL/p page size (=S)
 * kN/Sf + wL/S estimate for p
 *
 * If we run out of either heap or table, we increase p by 50%.
 * N/pf must be at least 100 to avoid full tables, S can increase if nessesary.
 */

struct NCDStats {
    number_of_keys: u64,
    total_length: u64
}

impl NCDStats {
    fn new(source: &dyn NCDValueSource) -> Result<NCDStats,NCDError> {
        let mut number_of_keys = 0;
        let mut total_length = 0;
        for key_value in wrap_io_error(source.iter())? {
            let (key,value) = wrap_io_error(key_value)?;
            number_of_keys += 1;
            total_length += (key.len() + value.len() + 6) as u64;
        }
        Ok(NCDStats { number_of_keys, total_length })
    }

    #[cfg(test)]
    fn new_values(number_of_keys: u64, total_length: u64) -> NCDStats {
        NCDStats { number_of_keys, total_length }
    }
}

fn make_stamp() -> u32 {
    let start = SystemTime::now();
    let secs = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards").as_secs();
    (secs & 0xFFFFFFFF) as u32
}

fn guess_number_of_pages(config: &NCDBuildConfig, stats: &NCDStats) -> u64 {
    let pointer_size_k = if config.target_page_size < 65536 { 2. } else { 4. };
    let total_table_space_needed = pointer_size_k * stats.number_of_keys as f64 / config.target_load_factor;
    let total_heap_space_needed = config.heap_wiggle_room * stats.total_length as f64;
    let total_space_needed = total_table_space_needed + total_heap_space_needed;
    let reduced_page_size = config.target_page_size - (HEADER_SIZE as u32); // to ensure header space always available
    (total_space_needed as u64) / (reduced_page_size as u64) + 1
}

fn guess_entries_par_page(stats: &NCDStats, number_of_pages: u64) -> u64 {
    let mut entries_per_page = (stats.number_of_keys / number_of_pages).max(1);
    if entries_per_page < 1 { entries_per_page = 1; }
    entries_per_page
}

fn initial_header_guess(config: &NCDBuildConfig, stats: &NCDStats, stamp: u32) -> Result<(NCDHeader,u64),NCDError> {
    if stats.number_of_keys == 0 {
        return Ok((NCDHeader::new(1,HEADER_SIZE as u32,0,None,stamp)?,0));
    }
    let pointer_size_k = if config.target_page_size < 65536 { 2. } else { 4. };
    let number_of_pages = guess_number_of_pages(config,stats);
    let entries_per_page = guess_entries_par_page(stats,number_of_pages);
    let table_size_entries = (entries_per_page as f64 / config.target_load_factor as f64) as u32 + 1;
    let table_size_bytes = table_size_entries * (pointer_size_k as u32);
    let heap_size = (config.target_page_size - table_size_bytes - 4).max(HEADER_SIZE as u32);
    let external_minimum = config.external_trheshold * (heap_size as f64);
    Ok((NCDHeader::new(number_of_pages,heap_size,table_size_entries,config.force_header_size,stamp)?,(external_minimum as u64).max(16)))
}

pub struct NCDBuild<'a> {
    source: &'a dyn NCDValueSource,
    config: NCDBuildConfig,
    header: NCDHeader,
    threshold: u64,
    filename: PathBuf,
    failure_reason: String
}

impl<'a> NCDBuild<'a> {
    pub fn describe_attempt(&self) -> String {
        format!("{} pages",self.header.number_of_pages())
    }

    pub fn result(&self) -> &str { &self.failure_reason }

    fn crank_page_count(&self) -> Result<NCDHeader,NCDError> {
        let new_pages = self.header.number_of_pages() as f64 * self.config.rebuild_page_factor;
        NCDHeader::new(new_pages as u64,self.header.heap_size(),self.header.table_size_entries(),self.config.force_header_size,self.header.stamp())
    }

    #[cfg(test)]
    #[allow(unused)]
    pub(super) fn testharness_header(&mut self) -> &mut NCDHeader { &mut self.header }

    fn fix_table_full(&mut self) -> Result<(),NCDError> {
        self.header = self.crank_page_count()?;
        Ok(())
    }

    fn fix_heap_full(&mut self) -> Result<(),NCDError> {
        self.header = self.crank_page_count()?;
        Ok(())
    }

    pub fn attempt(&mut self) -> Result<bool,NCDError> {
        let mut writer = NCDWriteAttempt::new(&self.header,&self.filename,self.threshold)?;
        match writer.add_all(self.source) {
            Err(NCDError::TableFull) => {
                self.failure_reason = "table overflow".to_string();
                self.fix_table_full()?;
                return Ok(false);
            },
            Err(NCDError::HeapFull) => { 
                self.failure_reason = "heap overflow".to_string();
                self.fix_heap_full()?;
                return Ok(false);
            },
            Err(e) => { return Err(e); },
            Ok(()) => { 
                self.failure_reason = format!("{0} pages with {1} entry, {2}-byte hash-table entries and heap of {3} bytes",
                                        self.header.number_of_pages(),self.header.table_size_entries(),
                                        self.header.pointer_length(),self.header.heap_size());
                return Ok(true);
            }
        }
    }

    pub fn new(config: &NCDBuildConfig, source: &'a dyn NCDValueSource, filename: &Path) -> Result<NCDBuild<'a>,NCDError> {
        let stats = NCDStats::new(source)?;
        let (header,threshold) = initial_header_guess(config,&stats,make_stamp())?;
        Ok(NCDBuild { 
            source, header, threshold, config: config.clone(), filename: filename.to_path_buf(), 
            failure_reason: "uninitialized".to_string() })
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;
    use std::env::temp_dir;

    use crate::StdNCDReadMutAccessor;
    use crate::build::{NCDBuild, NCDBuildConfig};
    use crate::header::NCDHeader;
    use crate::read::{ NCDReader };
    use crate::sources::hashmap::NCDHashMapValueSource;
    use crate::test::{numeric_key_values, temporary_path};
    use crate::util::{NCDError, wrap_io_error};

    use super::{NCDStats, initial_header_guess};
    
    const COUNT : u32 = 1000;

    fn do_test_build_run(break_table: bool, break_heap: bool) -> Result<(),NCDError> {
        let tmp_dir = temp_dir();
        let tmp_filename = Path::new(&tmp_dir).join("test3.ncd");
        let source = NCDHashMapValueSource::new(numeric_key_values(COUNT));
        let mut builder = NCDBuild::new(&NCDBuildConfig::new().target_page_size(1024),&source,&tmp_filename)?;
        let header = builder.testharness_header();
        if break_table {
            *header =  NCDHeader::new(header.number_of_pages(),header.heap_size(),10,None,header.stamp())?;
        }
        if break_heap {
            *header =  NCDHeader::new(header.number_of_pages(),64,header.table_size_entries(),None,header.stamp())?;
        }
        loop {
            println!("Attempting to build: {}",builder.describe_attempt());
            let success = builder.attempt()?;
            if success { break }
        }
        /**/
        let mut file = wrap_io_error(File::open(tmp_filename))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file))?;
        let mut reader = NCDReader::new(std)?;
        let kv = numeric_key_values(COUNT);
        let mut k_sorted = kv.keys().collect::<Vec<_>>();
        k_sorted.sort();
        for k in k_sorted.iter() {
            let v = kv.get(*k).unwrap().to_vec();
            let value = reader.lookup(&k)?;
            assert_eq!(Some(v),value);
        }
        Ok(())
    }

    fn do_test_build() -> Result<(),NCDError> {
        do_test_build_run(false,false)?;
        do_test_build_run(true,false)?;
        do_test_build_run(false,true)?;
        Ok(())
    }

    // XXX document

    #[test]
    fn build_smoke() {
        do_test_build().unwrap();
    }

    fn do_test_build_zero() -> Result<(),NCDError> {
        let tmp_filename = wrap_io_error(temporary_path())?;
        let source = NCDHashMapValueSource::new(numeric_key_values(0));
        let mut builder = NCDBuild::new(&NCDBuildConfig::new().target_page_size(1024),&source,&tmp_filename)?;
        loop {
            println!("Attempting to build: {}",builder.describe_attempt());
            let success = builder.attempt()?;
            if success { break }
        }
        /**/
        let mut file = wrap_io_error(File::open(tmp_filename))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file))?;
        let mut reader = NCDReader::new(std)?;
        let kv = numeric_key_values(COUNT);
        let mut k_sorted = kv.keys().collect::<Vec<_>>();
        k_sorted.sort();
        for k in k_sorted.iter() {
            kv.get(*k).unwrap().to_vec();
            let value = reader.lookup(&k)?;
            assert_eq!(None,value);
        }
        Ok(())
    }
    // XXX delete temporary files
    // XXX separate accessor impls

    fn do_test_header_size_override(set: Option<u32>, expect: u32, big_page: bool) -> Result<(),NCDError> {
        let tmp_filename = wrap_io_error(temporary_path())?;
        let source = NCDHashMapValueSource::new(numeric_key_values(1000));
        let page_size = if big_page { 100000 } else { 10000 };
        let config = NCDBuildConfig::new().force_header_size(set).target_page_size(page_size);
        let mut builder = NCDBuild::new(&config,&source,&tmp_filename)?;
        let header = builder.testharness_header();
        assert_eq!(expect,header.pointer_length() as u32);
        Ok(())
    }

    #[test]
    fn test_build_zero() {
        do_test_build_zero().unwrap();
    }

    #[test]
    fn test_header_size_override() {
        do_test_header_size_override(None,4,true).unwrap();        
        do_test_header_size_override(None,2,false).unwrap();
        do_test_header_size_override(Some(2),4,true).unwrap();        
        do_test_header_size_override(Some(2),2,false).unwrap();
        do_test_header_size_override(Some(4),4,true).unwrap();        
        do_test_header_size_override(Some(4),4,false).unwrap();
    }

    fn do_test_stamp() -> Result<(),NCDError> {
        let tmp_dir = temp_dir();
        let tmp_filename = Path::new(&tmp_dir).join("test9.ncd");
        let source = NCDHashMapValueSource::new(numeric_key_values(COUNT));
        let mut builder = NCDBuild::new(&NCDBuildConfig::new().target_page_size(1024),&source,&tmp_filename)?;
        loop {
            println!("Attempting to build: {}",builder.describe_attempt());
            let success = builder.attempt()?;
            if success { break }
        }
        /**/
        let mut file = wrap_io_error(File::open(tmp_filename))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file))?;
        let mut reader = NCDReader::new(std)?;
        let header = reader.testharness_header();
        *header =  NCDHeader::new(header.number_of_pages(),header.heap_size(),header.table_size_entries(),None,0x99999999)?;
        let kv = numeric_key_values(COUNT);
        let mut k_sorted = kv.keys().collect::<Vec<_>>();
        k_sorted.sort();
        for k in k_sorted.iter() {
            let v = kv.get(*k).unwrap().to_vec();
            let value = reader.lookup(&k)?;
            assert_eq!(Some(v),value);
        }
        Ok(())
    }

    #[test]
    fn test_stamp() {
        match do_test_stamp() {
            Err(NCDError::WrongStamp) => {},
            _ => { assert!(false); }
        }
    }

    fn header_test(config: &NCDBuildConfig, number_of_keys: u64, length_each: u64) -> Result<(u64,u32,u32,u64),NCDError> {
        let stats = NCDStats::new_values(number_of_keys,number_of_keys*length_each);
        let (header,threshold) = initial_header_guess(config,&stats,0)?;
        if header.table_size_entries() > 0 {
            assert_eq!(32768,header.page_size());
        }
        Ok((header.number_of_pages(),header.heap_size(),header.table_size_entries(),threshold))
    }

    fn do_test_header() -> Result<(),NCDError> {
        let config = NCDBuildConfig::new();
        assert_eq!((395,31750,507,3175),header_test(&config, 100000, 100)?);
        assert_eq!((1,28,0,0),header_test(&config, 0, 0)?);
        assert_eq!((1,32758,3,3275),header_test(&config, 1, 0)?);
        assert_eq!((1,32722,21,3272),header_test(&config, 10, 10)?);
        assert_eq!((1,32362,201,3236),header_test(&config, 100, 100)?);
        assert_eq!((39,32758,3,3275),header_test(&config, 10, 100000)?);
        assert_eq!((17,9234,11765,923),header_test(&config, 100000, 1)?);
        Ok(())
    }

    #[test]
    fn test_header() {
        do_test_header().unwrap()
    }
}
