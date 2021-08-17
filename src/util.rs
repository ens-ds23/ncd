use std::{fmt::{self, Display}, fs::File, io::{self, Seek, SeekFrom, Write}, path::Path};

#[derive(Debug)]
pub enum NCDError {
    IOError(io::Error),
    CorruptNCDFile(String),
    BadUTF8Error,
    UnsupportedVersion(String),
    BadConfiguration(String),
    /* Should be purely internal */
    HeapFull,
    TableFull,
    WrongStamp
}

impl Display for NCDError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NCDError::IOError(e) => write!(f,"{}",e),
            NCDError::CorruptNCDFile(e) => write!(f,"Corrupt NCD file: {}",e),
            NCDError::BadUTF8Error => write!(f,"Bad UTF8"),
            NCDError::UnsupportedVersion(e) => write!(f,"Unsupported NCD version: {}",e),
            NCDError::BadConfiguration(e) => write!(f,"Bad configuration: {}",e),
            NCDError::HeapFull => write!(f,"Heap full"),
            NCDError::TableFull => write!(f,"Table full"),
            NCDError::WrongStamp => write!(f,"Wrong stamp")
        }
    }
}

macro_rules! chain {
    ($name:ident,$getter_name:ident,$size:ty,$obj:ty) => {
        #[allow(unused)]
        pub fn $name(&self, value: $size) -> $obj {
            let mut out = self.clone();
            out.$name = value;
            out
        }

        #[allow(unused)]
        pub fn $getter_name(&self) -> &$size {
            &self.$name
        }
    };
}

pub fn wrap_io_error<T>(value: io::Result<T>) -> Result<T,NCDError> {
    value.map_err(|e | NCDError::IOError(e))
}

pub(crate) fn write_zero_length_file(path: &Path) -> Result<(),io::Error> {
    File::create(path)?;
    Ok(())
}

const BLOCK_SIZE : usize = 65536;

pub(crate) fn write_blanks_to_file(file: &mut File, mut length: u64) -> Result<(),NCDError> {
    wrap_io_error(file.seek(SeekFrom::Start(0)))?;
    let mut blank = vec![0;BLOCK_SIZE];
    while length > 0 {
        if blank.len() as u64 > length {
            blank = vec![0;length as usize];
        }
        wrap_io_error(file.write_all(&blank))?;
        length -= blank.len() as u64;
    }
    Ok(())
}

pub(crate) fn extract_pattern(pattern: &str, index: usize, line: &str) -> (String,String) {
    if let Some((sep_start,_)) = line.match_indices(pattern).nth(index-1) {
        let sep_end = sep_start + pattern.len();
        (line[0..sep_start].to_string(),line[sep_end..].to_string())
    } else {
        (line.to_string(),String::new())
    }
}

pub(crate) fn extract_whitespace(index: usize, line: &str) -> (String,String) {
    let mut sep = false;
    let mut k = String::new();
    let mut v = String::new();
    let mut sep_count = 0;
    let mut target = &mut k;
    for c in line.chars() {
        if c.is_whitespace() {
            if !sep {
                /* New separator */
                sep_count += 1;
                sep = true;
            }
            if sep_count  == index {
                target = &mut v;
                /* Don't accumulate in main separator */
                continue;
            } 
        } else {
            sep = false;
        }
        target.push(c);
    }
    (k,v)
}

pub(crate) fn all_whitespace(line: &str) -> bool {
    line.chars().filter(|c| !c.is_whitespace()).count() == 0
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::{self, Read, Seek, SeekFrom}};
    use byteorder::ReadBytesExt;
    use tempfile::{tempfile};

    use crate::{test::{delete_if_exists, temporary_path}, util::{extract_pattern, write_zero_length_file}};

    use super::{BLOCK_SIZE, NCDError, extract_whitespace, wrap_io_error, write_blanks_to_file};

    fn check_blank_file_size(file: &mut File, size: usize) -> Result<(),io::Error> {
        file.seek(SeekFrom::Start(0))?;
        for _ in 0..size {
            let v = file.read_u8()?;
            assert_eq!(0,v);
        }
        let mut more = vec![];
        file.read_to_end(&mut more)?;
        assert_eq!(0,more.len());
        Ok(())
    }

    fn test_write_blank_file_size(size: usize) -> Result<(),NCDError> {
        let mut file = wrap_io_error(tempfile())?;
        write_blanks_to_file(&mut file,size as u64)?;
        wrap_io_error(check_blank_file_size(&mut file,size))?;
        Ok(())
    }

    fn do_test_write_blank_file() -> Result<(),NCDError> {
        test_write_blank_file_size(0)?;
        test_write_blank_file_size(1)?;
        test_write_blank_file_size(BLOCK_SIZE-1)?;
        test_write_blank_file_size(BLOCK_SIZE+1)?;
        test_write_blank_file_size(2*BLOCK_SIZE)?;
        test_write_blank_file_size(2*BLOCK_SIZE+1)?;
        test_write_blank_file_size(1)?;
        test_write_blank_file_size(0)?;
        Ok(())
    }

    #[test]
    fn test_write_blank_file() {
        do_test_write_blank_file().unwrap();
    }

    fn do_test_blank_zero_length_file() -> Result<(),io::Error> {
        let tmp_file = temporary_path()?;
        delete_if_exists(&tmp_file)?;
        assert!(!tmp_file.exists());
        write_zero_length_file(&tmp_file)?;
        assert!(tmp_file.exists());
        let mut file = File::open(&tmp_file)?;
        check_blank_file_size(&mut file,0)?;
        delete_if_exists(&tmp_file)?;
        Ok(())
    }

    #[test]
    fn test_blank_zero_length_file() {
        do_test_blank_zero_length_file().unwrap();
    }

    fn do_test_extract_whitespace(line: &str, index: usize, key: &str, value: &str) {
        let (cmp_key,cmp_value) = extract_whitespace(index,line);
        assert_eq!(cmp_key,key);
        assert_eq!(cmp_value,value);
    }

    fn do_test_extract_pattern(line: &str, pattern: &str, index: usize, key: &str, value: &str) {
        let (cmp_key,cmp_value) = extract_pattern(pattern,index,line);
        assert_eq!(cmp_key,key);
        assert_eq!(cmp_value,value);
    }

    #[test]
    fn test_extract_whitespace() {
        do_test_extract_whitespace("hello world",1,"hello","world");
        do_test_extract_whitespace("hello world today",2,"hello world","today");
        do_test_extract_whitespace("hello    world",1,"hello","world");
        do_test_extract_whitespace("hello  world    today",2,"hello  world","today");
        do_test_extract_whitespace("  hello world",1,"","hello world");
        do_test_extract_whitespace("  hello world",2,"  hello","world");
        do_test_extract_whitespace("  hello world  ",2,"  hello","world  ");
        do_test_extract_whitespace("  hello",2,"  hello","");
        do_test_extract_whitespace("  hello  ",2,"  hello","");
        do_test_extract_whitespace("",2,"","");
        do_test_extract_whitespace("",1,"","");
    }

    #[test]
    fn test_extract_pattern() {
        do_test_extract_pattern("hello world"," ",1,"hello","world");
        do_test_extract_pattern("hello world today"," ",1,"hello","world today");
        do_test_extract_pattern("hello  world"," ",1,"hello"," world");
        do_test_extract_pattern("hello world today"," ",2,"hello world","today");
        do_test_extract_pattern("hello  world"," ",2,"hello ","world");
        do_test_extract_pattern("hello world","o",2,"hello w","rld");
        do_test_extract_pattern("hello world","or",1,"hello w","ld");
        do_test_extract_pattern(" hello world"," ",1,"","hello world");
        do_test_extract_pattern("  hello world"," ",1,""," hello world");
        do_test_extract_pattern("  hello world"," ",2," ","hello world");
        do_test_extract_whitespace("",2,"","");
        do_test_extract_whitespace("",1,"","");
    }
}
