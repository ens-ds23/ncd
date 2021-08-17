use std::{io::{self, ErrorKind}, time::Duration};
use curl::easy::Easy;
use crate::NCDReadAccessor;

#[derive(Clone)]
pub struct CurlConfig {
    connect_timeout: Duration,
}

impl CurlConfig {
    pub fn new() -> CurlConfig {
        CurlConfig {
            connect_timeout: Duration::new(2,0),
        }
    }

    fn configure_easy(&self, easy: &mut Easy) -> Result<(),curl::Error> {
        easy.connect_timeout(self.connect_timeout)?;
        Ok(())
    }

    chain!(connect_timeout,get_connect_timeout,Duration,CurlConfig);
}

fn wrap_curl_error<T>(value: Result<T,curl::Error>) -> Result<T,io::Error> {
    value.map_err(|e| io::Error::new(ErrorKind::Other,e.to_string()))
}

pub struct CurlNCDReadAccessor {
    curl: Easy
}

enum ReadResponse {
    Data(Vec<u8>),
    HttpError(u32)
}

impl CurlNCDReadAccessor {
    pub fn new(config: &CurlConfig, url: &str) -> io::Result<CurlNCDReadAccessor> {
        let mut easy = Easy::new();
        wrap_curl_error(easy.url(url))?;
        wrap_curl_error(config.configure_easy(&mut easy))?;
        Ok(CurlNCDReadAccessor {
            curl: easy
        })
    }

    fn read_curl(&mut self, offset: u64, length: u64) -> Result<ReadResponse,curl::Error> {
        let mut data = vec![];
        if length == 0 { return Ok(ReadResponse::Data(data)); }
        self.curl.range(&format!("{}-{}",offset,offset+length-1))?;
        let mut transfer = self.curl.transfer();
        transfer.write_function(|more| {
            data.extend_from_slice(more);
            Ok(more.len())
        })?;
        transfer.perform()?;
        drop(transfer);
        let code = self.curl.response_code()?;
        if code > 299 {
            return Ok(ReadResponse::HttpError(code));
        }
        Ok(ReadResponse::Data(data))
    }
}

impl NCDReadAccessor for CurlNCDReadAccessor {
    fn read(&mut self, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        match wrap_curl_error(self.read_curl(offset,length))? {
            ReadResponse::Data(d) => Ok(d),
            ReadResponse::HttpError(e) => {
                Err(io::Error::new(ErrorKind::Other,format!("HTTP error code={}",e)))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{io, time::Duration};

    use crate::{NCDError, NCDReadAccessor, test::SMOKE_FILE, wrap_io_error};

    use super::{CurlNCDReadAccessor, CurlConfig};

    const URL : &str = "https://raw.githubusercontent.com/ens-ds23/ncd/main/testdata/smoke.ncd";
    const BAD_URLS : &[(&str,&str)] = &[
        ("https://192.168.255.255:12345/does-not-exist.ncd","Timeout was reached"),
        ("https://raw.githubusercontent.com/ens-ds23/ncd/main/testdata/404.ncd","404")
    ];

    fn do_test_curl() -> Result<(),NCDError> {
        let mut curl = wrap_io_error(CurlNCDReadAccessor::new(&CurlConfig::new(),URL))?;
        for offset in [0_usize,7,9,12] {
            for length in [0_usize,5,8,14,24] {
                let read = wrap_io_error(curl.read(offset as u64,length as u64))?;
                if length != 0 {
                    assert_eq!(&SMOKE_FILE[offset..(offset+length)],read);
                } else {
                    assert_eq!(read.as_slice(),&[]);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_curl() {
        do_test_curl().unwrap()
    }

    fn try_url(url: &str) -> Result<(),io::Error> {
        let config = CurlConfig::new().connect_timeout(Duration::new(2,0));
        let mut curl = CurlNCDReadAccessor::new(&config,url)?;
        curl.read(0,8)?;
        Ok(())
    }

    fn do_test_curl_bad_url() -> Result<(),NCDError> {
        for (bad_url,find) in BAD_URLS {
            match try_url(bad_url) {
                Ok(()) => { assert!(false); },
                Err(e) => { 
                    println!("{}",e.to_string());
                    assert!(e.to_string().contains(find)); 
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_curl_bad_url() {
        do_test_curl_bad_url().unwrap()
    }
}
