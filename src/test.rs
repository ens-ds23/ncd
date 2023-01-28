use std::collections::HashMap;
use std::fs::{File, remove_file};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use byteorder::{LittleEndian, WriteBytesExt};
use rand::{self, Rng};
use tempfile::{ TempDir };
use crate::build::{NCDBuild, NCDBuildConfig};
use crate::header::NCDHeader;
use crate::sources::hashmap::NCDHashMapValueSource;
use crate::util::{NCDError, wrap_io_error};
use crate::write::NCDValueSource;

pub(crate) fn temporary_path() -> Result<PathBuf,io::Error> {
    let start = SystemTime::now();
    let now = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let tmp_dir = TempDir::new()?;
    let tmpname = format!("ncd-tmp-{:x}",now.as_millis());
    let tmp_file = tmp_dir.into_path().join(Path::new(&tmpname));
    Ok(tmp_file)
}

/* has race, ok for testing, don't use in library proper */
pub(crate) fn delete_if_exists(path: &Path) -> io::Result<()> {
    if path.exists() {
        remove_file(path)?;
    }
    Ok(())
}

pub(crate) const SMOKE_FILE : &[u8] = include_bytes!("../testdata/smoke.ncd");

pub(crate) fn numeric_key_values(limit: u32) -> HashMap<Vec<u8>,Vec<u8>> {
    let mut out = HashMap::new();
    for i in 0..limit {
        let k = format!("{}",i).as_bytes().to_vec();
        let v = if i%10 == 0 {
            format!("----------{}----------",limit-i).as_bytes().to_vec()
        } else {
            format!("{}",limit-i).as_bytes().to_vec()
        };
        out.insert(k,v);
    }
    out
}

pub(crate) fn fuzz_scratch() -> Vec<u8> {
    let len = rand::random::<u16>() as usize;
    let mut out = vec![0;len];
    for i in 0..len {
        out[i] = rand::random::<u8>()
    }
    out
}

pub(crate) fn example_file() -> Result<Vec<u8>,NCDError> {
    const COUNT : u32 = 1000;

    let path = wrap_io_error(temporary_path())?;
    let source = NCDHashMapValueSource::new(numeric_key_values(COUNT));
    let mut builder = NCDBuild::new(&NCDBuildConfig::new().target_page_size(1024),&source,&path)?;
    loop {
        println!("Attempting to build: {}",builder.describe_attempt());
        let success = builder.attempt(|_,_| {})?;
        if success { break }
    }
    /**/
    let mut file = wrap_io_error(File::open(path))?;
    let mut buf = vec![];
    wrap_io_error(file.read_to_end(&mut buf))?;
    Ok(buf)
}

pub(crate) fn update_header_stamp(file: &mut File, header: &NCDHeader, stamp: u32) -> Result<(),NCDError> {
    // XXX crank to header
    let header = NCDHeader::new(header.number_of_pages(),header.heap_size(),header.table_size_entries(),Some(4),stamp)?;
    header.write(file)?;
    wrap_io_error(file.flush())?;
    Ok(())
}

pub(crate) fn update_table_stamp(file: &mut File, header: &NCDHeader, index: u64, stamp: u32) -> Result<(),NCDError> {
    let file_offset = header.stamp_offset(index);
    wrap_io_error(file.seek(SeekFrom::Start(file_offset)))?;
    wrap_io_error(file.write_u32::<LittleEndian>(stamp))?;
    wrap_io_error(file.flush())?;
    Ok(())
}

pub(crate) fn tinker_with_data(data: &mut [u8]) {
    let mut rng = rand::thread_rng();
    for _ in 0..rng.gen::<u16>() {
        data[rng.gen_range(0..data.len())] = rng.gen::<u8>();
    }
}

pub(crate) fn extract_all(source: &dyn NCDValueSource) -> Result<Vec<(Vec<u8>,Vec<u8>)>,NCDError> {
    let mut out = vec![];
    for item in wrap_io_error(source.iter())? {
        out.push(wrap_io_error(item)?);
    }
    Ok(out)
}
