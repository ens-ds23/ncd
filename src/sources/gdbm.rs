use std::{io::{self, ErrorKind}, path::Path};
use gnudbm::{GdbmOpener, Iter, ReadHandle};

use crate::write::{NCDValueSource};

fn wrap_db_error<T>(value: Result<T,gnudbm::Error>) -> io::Result<T> {
    value.map_err(|e| io::Error::new(ErrorKind::Other,e.to_string()))
}

pub struct NCDGdbmIterator<'a> {
    iter: Iter<'a>
}

impl<'a> NCDGdbmIterator<'a> {
    fn new(db: &'a ReadHandle) -> NCDGdbmIterator<'a> {
        NCDGdbmIterator {
            iter: db.iter()
        }
    }
}

impl<'a> Iterator for NCDGdbmIterator<'a> {
    type Item = io::Result<(Vec<u8>,Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self.iter.next();
        if record.is_none() { return None; }
        let (key,entry) = record.unwrap();
        return Some(Ok((key.as_bytes().to_vec(),entry.as_bytes().to_vec())));
    }
}

pub struct NCDGdbmSource {
    db: ReadHandle
}

impl NCDGdbmSource {
    pub fn new(path: &Path) -> io::Result<NCDGdbmSource> {
        let db = wrap_db_error(GdbmOpener::new().readonly(path))?;
        Ok(NCDGdbmSource { db })
    }
}

impl NCDValueSource for NCDGdbmSource {
    fn iter<'a>(&'a self) -> io::Result<Box<dyn Iterator<Item=io::Result<(Vec<u8>,Vec<u8>)>> + 'a>> {
        Ok(Box::new(NCDGdbmIterator::new(&self.db)))
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, path::{Path}};

    use gnudbm::GdbmOpener;

    use crate::{StdNCDReadMutAccessor, build::{NCDBuild, NCDBuildConfig}, read::NCDReader, test::temporary_path, util::{NCDError, wrap_io_error}};

    use super::{NCDGdbmSource, wrap_db_error};

    fn do_test_gdbm() -> Result<(),NCDError> {
        let source_filename = Path::new(file!()).to_path_buf().parent().unwrap().join(Path::new("../../testdata/test.gdbm"));
        println!("source {:?}",source_filename.as_os_str());
        let source = wrap_io_error(NCDGdbmSource::new(&Path::new(&source_filename)))?;
        let tmp_filename = wrap_io_error(temporary_path())?;
        let mut builder = NCDBuild::new(&NCDBuildConfig::new().target_page_size(16384).heap_wiggle_room(1.1).target_load_factor(0.75).rebuild_page_factor(1.1),&source,&tmp_filename)?;
        loop {
            println!("Attempting to build: {}",builder.describe_attempt());
            let success = builder.attempt()?;
            if success { break }
            println!("  {}",builder.result());
        }
        drop(builder);
        let mut tmp_file = wrap_io_error(File::open(&tmp_filename))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut tmp_file))?;
        let mut reader = NCDReader::new(std)?;
        let db = wrap_io_error(wrap_db_error(GdbmOpener::new().readonly(source_filename)))?;
        for (key,value) in db.iter() {
            let key = key.as_bytes();
            let gbbm_value = value.as_bytes();
            let ncd_value = reader.lookup(key)?;       
            assert_eq!(ncd_value,Some(gbbm_value.to_vec()));
        }
        Ok(())
    }
    
    #[test]
    fn test_gdbm() {
        do_test_gdbm().unwrap()
    }
}
