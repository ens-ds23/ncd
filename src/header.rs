use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use crate::NCDBuildConfig;
use crate::bitbash::{all_set, read_u32, read_u64, write_u32, write_u64};
use crate::read::{NCDReadAccessor};
use crate::util::{NCDError, wrap_io_error};

// chosen to very much repel text tools (and an N).
pub const MAGIC_NUMBER : u32 = 0x4E00C0FE; // be->le byteswapped, so first byte in file is 0xFE

pub struct NCDHeader {
    #[allow(unused)]
    version: u32,
    number_of_pages: u64,
    heap_size: u32,
    table_size: u32,
    stamp: u32
}

pub(crate) const HEADER_SIZE : usize = 28;

impl NCDHeader {
    pub fn new(number_of_pages: u64, heap_size: u32, table_size: u32, force_header_size: Option<u32>, stamp: u32) -> Result<NCDHeader,NCDError> {
        let min_header_size = if heap_size + table_size + 4 < 65536 {2} else {4};
        let header_size = force_header_size.unwrap_or(min_header_size).max(min_header_size);
        let version = match  header_size {
            2 => 1,
            4 => 0,
            x => { return Err(NCDError::BadConfiguration(format!("unsupported header size {}",x)))}
        };
        Ok(NCDHeader {
            version,
            number_of_pages,
            heap_size,
            table_size,
            stamp
        })
    }

    pub fn read(accessor: &mut dyn NCDReadAccessor) -> Result<NCDHeader,NCDError> {
        let vec = wrap_io_error(accessor.read(0, HEADER_SIZE as u64))?;
        let bytes = vec.as_slice();
        let mut offset = 0;
        let magic_number = read_u32(bytes,&mut offset)?;
        let version = read_u32(bytes,&mut offset)?;
        let number_of_pages = read_u64(bytes,&mut offset)?;
        let heap_size = read_u32(bytes,&mut offset)?;
        let table_size = read_u32(bytes,&mut offset)?;
        let stamp = read_u32(bytes,&mut offset)?;
        if magic_number != MAGIC_NUMBER {
            return Err(NCDError::CorruptNCDFile(format!("Bad magic number {0:x}, not an NCD file",magic_number)));
        }
        if version > 1 {
            return Err(NCDError::CorruptNCDFile(format!("Unsupported version {}",version)));
        }
        let out = NCDHeader { version, number_of_pages, heap_size, table_size, stamp };
        let page_size_check = table_size as u64 * out.pointer_length() as u64 + heap_size as u64 + 4;
        if page_size_check > 0xFFFFFFFF {
            return Err(NCDError::CorruptNCDFile(format!("Pages too big")));
        }
        Ok(out)
    }

    pub fn write(&self, file: &mut File) -> Result<(),NCDError> {
        let mut bytes = vec![0;HEADER_SIZE];
        let mut offset = 0;
        write_u32(&mut bytes,&mut offset,MAGIC_NUMBER)?;
        write_u32(&mut bytes,&mut offset,self.version)?;
        write_u64(&mut bytes,&mut offset,self.number_of_pages)?;
        write_u32(&mut bytes,&mut offset,self.heap_size)?;
        write_u32(&mut bytes,&mut offset,self.table_size)?;
        write_u32(&mut bytes,&mut offset,self.stamp)?;
        wrap_io_error(file.seek(SeekFrom::Start(0)))?;
        wrap_io_error(file.write_all(&bytes))?;
        Ok(())
    }

    pub fn pointer_length(&self) -> usize { if self.version == 1 { 2 } else { 4 } }
    pub fn table_size_entries(&self) -> u32 { self.table_size }
    pub fn number_of_pages(&self) ->u64 { self.number_of_pages }
    pub fn page_size(&self) -> u32 { 4 + self.table_size * (self.pointer_length() as u32) + self.heap_size }
    pub fn heap_size(&self) -> u32 { self.heap_size }
    pub fn stamp(&self) -> u32 { self.stamp }

    pub fn unused_value(&self) -> Result<u64,NCDError> { all_set(self.pointer_length()) }

    pub fn page_offset(&self, page_index: u64) -> u64 {
        (self.page_size() as u64) * page_index
    }

    pub fn table_offset(&self, page_index: u64) -> u64 {
        self.page_offset(page_index) + (self.heap_size as u64)
    }

    pub fn stamp_offset(&self, page_index: u64) -> u64 {
        self.page_offset(page_index+1) - 4
    }

    pub fn hash_page_index(&self, hash: u64) -> u64 {
        if self.table_size == 0 { return 0; }
        ((hash/self.table_size as u64) % self.number_of_pages as u64) as u64
    }

    pub fn hash_page_slot(&self, hash: u64) -> u32 {
        (hash%self.table_size as u64) as u32
    }

    pub fn hash_ext(&self, hash: u64) -> u32 {
        if self.table_size == 0 || self.number_of_pages == 0 { return 0; }
        ((hash/(self.table_size as u64)/(self.number_of_pages as u64)) &0xFFFFFFFF) as u32
    }

    pub(crate) fn structured_size(&self) -> u64 { self.page_offset(self.number_of_pages) }
}
