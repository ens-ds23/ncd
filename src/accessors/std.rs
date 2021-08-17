use std::{io::{self, Read, Seek, SeekFrom}};

use crate::{read::NCDReadAccessor};

pub struct StdNCDReadMutAccessor<'a,T> where T: Read+Seek {
    inner: &'a mut T
}

impl<'a,T> StdNCDReadMutAccessor<'a,T> where T: Read+Seek {
    pub fn new(inner: &'a mut T) -> io::Result<StdNCDReadMutAccessor<'a,T>> {
        Ok(StdNCDReadMutAccessor {
            inner
        })
    }
}

impl<'a,T> NCDReadAccessor for StdNCDReadMutAccessor<'a,T> where T: Read+Seek {
    fn read(&mut self, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        self.inner.seek(SeekFrom::Start(offset))?;
        let mut out = vec![];
        self.inner.take(length).read_to_end(&mut out)?;
        Ok(out)
    }
}

pub struct StdNCDReadAccessor<T> where T: Read+Seek {
    inner: Box<T>
}

impl<T> StdNCDReadAccessor<T> where T: Read+Seek {
    pub fn new(inner: T) -> io::Result<StdNCDReadAccessor<T>> {
        Ok(StdNCDReadAccessor {
            inner: Box::new(inner)
        })
    }
}

impl<T> NCDReadAccessor for StdNCDReadAccessor<T> where T: Read+Seek {
    fn read(&mut self, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        self.inner.seek(SeekFrom::Start(offset))?;
        let mut out = vec![];
        self.inner.as_mut().take(length).read_to_end(&mut out)?;
        Ok(out)
    }
}
