use std::io;

use crate::bitbash::{bounds_check, lesqlite2_read, read_bytes, read_u32, read_uvar};
use crate::util::{NCDError, wrap_io_error};
use crate::{bitbash::compute_hash, header::{ NCDHeader }};

pub trait NCDReadAccessor {
    fn read(&mut self, offset: u64, length: u64) -> io::Result<Vec<u8>>;
}

// XXX async
// XXX condioitnal debugs

#[cfg_attr(debug_assertions,derive(Debug,PartialEq,Eq))]
pub(crate) enum NCDLookupResult {
    Internal(Vec<u8>,Vec<u8>),
    External(u64,u64,u32),
    Empty
}

#[cfg_attr(debug_assertions,derive(Debug,PartialEq,Eq))]
enum NCDLookupEntry {
    Value(Vec<u8>),
    Skip,
    Finish
}

impl NCDLookupResult {
    fn resolve(self, reader: &mut NCDFileReader, key: &[u8]) -> Result<NCDLookupEntry,NCDError> {
        match self {
            NCDLookupResult::Internal(k,v) => {
                if key == k {
                    return Ok(NCDLookupEntry::Value(v));
                } else {
                    return Ok(NCDLookupEntry::Skip);
                }
            },
            NCDLookupResult::Empty => Ok(NCDLookupEntry::Finish),
            NCDLookupResult::External(offset,size,hash) => {
                let key_hash = reader.header().hash_ext(compute_hash(key)?);
                if key_hash != hash {
                    return Ok(NCDLookupEntry::Skip);
                }
                let bytes = wrap_io_error(reader.accessor().read(offset,size))?;
                let entry = parse_entry(&bytes, 0)?;
                match entry {
                    NCDLookupResult::Internal(k,v) => {
                        if k == key {
                            return Ok(NCDLookupEntry::Value(v));
                        } else {
                            return Ok(NCDLookupEntry::Skip);
                        }
                    },
                    NCDLookupResult::Empty => Ok(NCDLookupEntry::Finish),
                    NCDLookupResult::External(_,_,_) => {
                        return Err(NCDError::CorruptNCDFile(format!("recursive external reference")))
                    }        
                }
            }
        }
    }
}

pub(crate) fn parse_entry(heap: &[u8], offset: usize) -> Result<NCDLookupResult,NCDError> {
    let mut offset = offset;
    let key_len = lesqlite2_read(heap,&mut offset)?;
    if key_len == 0 {
        /* external */
        let ext_offset = lesqlite2_read(heap,&mut offset)?;
        let ext_length = lesqlite2_read(heap,&mut offset)?;
        bounds_check(heap,offset,4)?;
        let hash = read_u32(heap, &mut offset)?;
        return Ok(NCDLookupResult::External(ext_offset,ext_length,hash));
    } else {
        /* internal */
        let key_len = (key_len-1) as usize;
        bounds_check(heap,offset,key_len)?;
        let key = read_bytes(heap, &mut offset, key_len)?.to_vec();
        let value_len = lesqlite2_read(heap,&mut offset)? as usize;
        bounds_check(heap,offset,value_len)?;
        let value = read_bytes(heap,&mut offset,value_len)?.to_vec();
        return Ok(NCDLookupResult::Internal(key,value));
    }
}

struct NCDPage {
    heap: Vec<u8>,
    table: Vec<Option<u64>>
}

impl NCDPage {
    fn read(accessor: &mut dyn NCDReadAccessor, header: &NCDHeader, index: u64) -> Result<NCDPage,NCDError> {
        let page_size = header.page_size() as u64;
        if header.table_size_entries() == 0 {
            return Ok(NCDPage { heap: vec![], table: vec![] });
        }
        let vec = wrap_io_error(accessor.read(page_size*index,page_size))?;
        let bytes = vec.as_slice();
        let mut table = vec![];
        table.reserve(header.table_size_entries() as usize);
        let mut offset = header.heap_size() as usize;
        let unused_value = header.unused_value()?;
        for _ in 0..header.table_size_entries() {
            let value = read_uvar(bytes,&mut offset,header.pointer_length())?;
            let value = if value == unused_value { None } else { Some(value) };
            table.push(value);
        }
        let mut offset = (page_size-4) as usize;
        let stamp = read_u32(bytes,&mut offset)?;
        if stamp != header.stamp() {
            return Err(NCDError::WrongStamp);
        }
        Ok(NCDPage {
            heap: bytes[0..(header.heap_size() as usize)].to_vec(),
            table
        })
    }

    fn lookup(&self, index: u32) -> Result<NCDLookupResult,NCDError> {
        if index as usize >= self.table.len() {
            return Err(NCDError::CorruptNCDFile(format!("bad index {}",index)));
        }
        let offset = self.table[index as usize];
        if offset.is_none() {
            return Ok(NCDLookupResult::Empty);
        }
        let offset = offset.unwrap() as usize;
        parse_entry(&self.heap,offset)
    }

    fn scan(&self, reader: &mut NCDFileReader, key: &[u8], hash: u64) -> Result<Option<Vec<u8>>,NCDError> {
        let header = reader.header();
        if header.table_size_entries() == 0 {
            return Ok(None);
        }
        let mut hash = header.hash_page_slot(hash);
        let first_hash = hash;
        loop {
            let result = self.lookup(hash)?.resolve(reader,key)?;
            match result {
                NCDLookupEntry::Value(value) => { return Ok(Some(value)); },
                NCDLookupEntry::Finish => { return Ok(None); },
                NCDLookupEntry::Skip => {}
            }
            hash = (hash+1) % reader.header().table_size_entries();
            if hash == first_hash { return Ok(None); }
        }
    }
}

pub struct NCDFileReader<'a> {
    reader: Box<dyn NCDReadAccessor + 'a>,
    header: NCDHeader
}

impl<'a> NCDFileReader<'a> { // XXX rename
    pub fn new_box(mut reader: Box<dyn NCDReadAccessor + 'a>) -> Result<NCDFileReader<'a>,NCDError> {
        let header = NCDHeader::read(reader.as_mut())?;
        Ok(NCDFileReader { reader, header })
    }

    pub fn new<T>(reader: T) -> Result<NCDFileReader<'a>,NCDError> where T: NCDReadAccessor + 'a {
        Self::new_box(Box::new(reader))
    }

    #[cfg(test)]
    pub(super) fn testharness_header(&mut self) -> &mut NCDHeader { &mut self.header }

    pub fn accessor(&mut self) -> &mut dyn NCDReadAccessor { self.reader.as_mut() }
    pub fn header(&self) -> &NCDHeader { &self.header }
    fn page(&mut self, index: u64) -> Result<NCDPage,NCDError> { NCDPage::read(self.reader.as_mut(),&self.header,index) }

    pub fn lookup(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>,NCDError> {
        let hash = compute_hash(key)?;
        let page_index = self.header.hash_page_index(hash);
        let page = self.page(page_index)?;
        page.scan(self,key,hash)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>,NCDError> {
        loop {
            match self.lookup(key) {
                Err(NCDError::WrongStamp) => {
                    let new_header = NCDHeader::read(self.reader.as_mut())?;
                    if new_header.stamp() == self.header().stamp() {
                        return Err(NCDError::WrongStamp);
                    }
                    self.header = new_header;
                },
                x => { return x; }                
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{env::temp_dir, fs::{File, OpenOptions}, io::{BufWriter, Write}, path::Path};

    use tempfile::{NamedTempFile, tempfile};

    use crate::{StdNCDReadMutAccessor, bitbash::{compute_hash, write_u32}, header::MAGIC_NUMBER, read::{NCDFileReader, NCDLookupEntry, NCDLookupResult}, test::{SMOKE_FILE, delete_if_exists, example_file, fuzz_scratch, tinker_with_data, update_header_stamp, update_table_stamp}, util::{NCDError, wrap_io_error}};

    fn do_file_read_smoke() -> Result<(),NCDError> {
        let tmp_dir = temp_dir();
        let tmp_filename = Path::new(&tmp_dir).join("test.ncd");
        wrap_io_error(delete_if_exists(&tmp_filename))?;
        let mut tmp_file = BufWriter::new(wrap_io_error(File::create(tmp_filename.clone()))?);
        wrap_io_error(tmp_file.write_all(SMOKE_FILE))?;
        wrap_io_error(tmp_file.flush())?;
        drop(tmp_file);
        let mut file = wrap_io_error(File::open(tmp_filename))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file))?;
        let mut reader = NCDFileReader::new(std)?;
        let page = reader.page(0)?;
        let value = page.lookup(1)?;
        assert_eq!(&value,&NCDLookupResult::Internal(b"Hello".to_vec(),b"World".to_vec()));
        let value = page.lookup(0)?;
        match value {
            NCDLookupResult::External(_,_,_) => {},
            _ => { assert!(true); }
        }
        let value = page.lookup(0)?.resolve(&mut reader,b"Goodbye")?;
        assert_eq!(NCDLookupEntry::Value(b"Mars".to_vec()),value);
        let value = page.lookup(2)?.resolve(&mut reader,b"e")?;
        assert_eq!(NCDLookupEntry::Value(b"f".to_vec()),value);
        let value = page.scan(&mut reader,b"e",compute_hash(b"e")?)?;
        assert_eq!(value,Some(b"f".to_vec()));
        assert_eq!(Some(b"World".to_vec()),reader.lookup(b"Hello")?);
        assert_eq!(Some(b"Mars".to_vec()),reader.lookup(b"Goodbye")?);
        assert_eq!(None,reader.lookup(b"v")?);
        Ok(())
    }

    #[test]
    fn file_read_smoke() {
        do_file_read_smoke().unwrap();
    }

    const ROUNDS : usize = 100;

    fn do_fuzz_write_scratch() -> Result<(),NCDError> {
        let mut data = fuzz_scratch();
        if data.len() > 8 { // Set magic number to make it more of a challenge!
            let short = data.len()%3==0;
            let mut offset = 0;
            write_u32(&mut data, &mut offset, MAGIC_NUMBER)?;
            write_u32(&mut data, &mut offset, if short {1} else {0})?;
        }
        let mut temp_file = wrap_io_error(tempfile())?; 
        wrap_io_error(temp_file.write_all(&data))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut temp_file))?;
        let mut reader = NCDFileReader::new(std)?;
        let page = reader.page(0)?;
        page.lookup(1)?;       
        Ok(())      
    }

    fn do_fuzz_write_tinker(pure: &[u8], tinker: bool) -> Result<(),NCDError> {
        let mut tinkered = pure.to_vec();
        if tinker {
            tinker_with_data(&mut tinkered);
        }
        let mut temp_file = wrap_io_error(tempfile())?; 
        wrap_io_error(temp_file.write_all(&tinkered))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut temp_file))?;
        let mut reader = NCDFileReader::new(std)?;
        let page = reader.page(0)?;
        page.lookup(1)?;
        Ok(())
    }

    #[test]
    fn fuzz_write_scratch() {
        for _ in 0..ROUNDS {
            do_fuzz_write_scratch().ok();
        }
    }

    fn fuzz_write_tinker_loop() -> Result<(),NCDError> {
        let data = example_file()?;
        assert!(do_fuzz_write_tinker(&data,false).is_ok());
        for _ in 0..ROUNDS {
            do_fuzz_write_tinker(&data,true).ok();
        }
        Ok(())
    }

    // XXX tiny files

    #[test]
    fn fuzz_write_tinker() {
        fuzz_write_tinker_loop().unwrap();
    }

    fn do_test_stamp_change(update_table: bool, update_header: bool, success: bool) -> Result<(),NCDError> {
        let mut file1 = wrap_io_error(NamedTempFile::new())?;
        let mut file2 = wrap_io_error(OpenOptions::new().read(true).write(true).open(file1.path()))?;
        wrap_io_error(file1.write_all(&SMOKE_FILE))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file1))?;
        let mut reader = NCDFileReader::new(std)?;
        if update_header {
            update_header_stamp(&mut file2,&reader.header(),12345)?;
        }
        if update_table {
            update_table_stamp(&mut file2,&reader.header(),0,12345)?;
        }
        let v = reader.get(b"Hello");
        println!("{:?}",v);
        let ok = match reader.get(b"1") {
            Ok(_) => success,
            Err(NCDError::WrongStamp) => !success,
            _ => false
        };
        assert!(ok);
        Ok(())
    }

    #[test]
    fn test_stamp_change() {
        do_test_stamp_change(false,false,true).unwrap();
        do_test_stamp_change(true,false,false).unwrap();
        do_test_stamp_change(true,true,true).unwrap();
    }
}
