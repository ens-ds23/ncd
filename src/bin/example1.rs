use std::{fmt::Display, io::{Seek, SeekFrom, Write}, process};
use tempfile::{NamedTempFile};

use ncd::{NCDBuild, NCDBuildConfig, NCDFileReader, NCDFlatConfig, NCDFlatSource, StdNCDReadMutAccessor};

fn die<E: Display>(value: E) -> ! {
    eprintln!("{}",value);
    process::exit(1);
}

fn die_on_error<T,E: Display>(value: Result<T,E>) -> T {
    match value {
        Ok(v) => v,
        Err(e) => die(e)
    }
}

/* create from flat file */
fn main() {
    let flat_data = include_bytes!("../../testdata/test_flat.txt");
    let mut in_file = die_on_error(NamedTempFile::new());
    die_on_error(in_file.seek(SeekFrom::Start(0)));
    die_on_error(in_file.write_all(flat_data));
    let mut file = die_on_error(NamedTempFile::new());
    let source = die_on_error(NCDFlatSource::new(in_file.path(),&NCDFlatConfig::new()));
    let mut builder = die_on_error(NCDBuild::new(&NCDBuildConfig::new().target_page_size(1024),&source,&file.path()));
    loop {
        println!("Attempting to build: {}",builder.describe_attempt());
        let success = die_on_error(builder.attempt());
        if success { break }
    }
    let std = die_on_error(StdNCDReadMutAccessor::new(&mut file));
    let mut reader = die_on_error(NCDFileReader::new(std));
    let value = die_on_error(reader.lookup(b"hello"));
    match value {
        Some(value) => { println!("hello {}",die_on_error(String::from_utf8(value))); }
        None => { println!("missing key"); }
    }
}
