use std::{fs::File, io::{self, BufRead, BufReader, Lines}, path::{Path, PathBuf}};

use crate::{util::{all_whitespace, extract_pattern, extract_whitespace}, write::{NCDValueSource}};

#[derive(Clone)]
pub struct NCDFlatConfig {
    index: usize,
    separator: Option<String>,
    skip_blank: bool,
    comment_char: Option<String>,
    inline_comments: bool,
    trim_tail: bool
}

impl NCDFlatConfig {
    pub fn new() -> NCDFlatConfig {
        NCDFlatConfig {
            index: 1,
            separator: None,
            skip_blank: true,
            comment_char: None,
            inline_comments: false,
            trim_tail: true
        }
    }

    chain!(index,get_index,usize,NCDFlatConfig);
    chain!(separator,get_separator,Option<String>,NCDFlatConfig);
    chain!(skip_blank,get_skip_blank,bool,NCDFlatConfig);
    chain!(comment_char,get_comment_char,Option<String>,NCDFlatConfig);
    chain!(inline_comments,get_inline_comments,bool,NCDFlatConfig);
    chain!(trim_tail,get_trim_tail,bool,NCDFlatConfig);
}

pub struct NCDFlatIterator {
    lines: Lines<BufReader<File>>,
    config: NCDFlatConfig
}

impl<'a> NCDFlatIterator {
    fn new(path: &'a PathBuf, config: &NCDFlatConfig) -> io::Result<NCDFlatIterator> {
        Ok(NCDFlatIterator {
            lines: BufReader::new(File::open(path)?).lines(),
            config: config.clone()
        })
    }

    fn remove_comments<'b>(&self, mut line: &'b str) -> Option<&'b str> {
        if let Some(comment_char) = &self.config.comment_char {
            if let Some((index,_)) = line.match_indices(comment_char).next() {
                let comment_is_prefix = line[0..index].matches(|c: char| !c.is_whitespace()).next().is_none();
                if comment_is_prefix || self.config.inline_comments {
                    line = &line[0..index];
                }
            }
        }
        if self.config.trim_tail {
            if let Some((index,_)) = line.match_indices(|c: char| !c.is_whitespace()).rev().next() {
                line = &line[..(index+1)];
            }
        }
        if self.config.skip_blank && all_whitespace(line) { return None; }
        Some(line)
    }

    fn extract_sep(&self, line: &str, sep: &str) -> (Vec<u8>,Vec<u8>) {
        let (key,value) = extract_pattern(sep,self.config.index,line);
        (key.as_bytes().to_vec(),value.as_bytes().to_vec())
    }

    fn extract_whitespace(&self, line: &str) -> (Vec<u8>,Vec<u8>) {
        let (key,value) = extract_whitespace(self.config.index,line);
        (key.as_bytes().to_vec(),value.as_bytes().to_vec())
    }

    fn extract(&self, line: &str) -> Option<(Vec<u8>,Vec<u8>)> {
        let line = self.remove_comments(line);
        if let Some(line) = line {
            Some(match &self.config.separator {
                Some(sep) => self.extract_sep(line,sep),
                None => self.extract_whitespace(line)
            })
        } else { None }
    }
}

impl Iterator for NCDFlatIterator {
    type Item = io::Result<(Vec<u8>,Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_line = self.lines.next();
            if next_line.is_none() { return None; }
            let values = next_line.unwrap().map(|v| { self.extract(&v) });
            match values {
                Ok(Some(values)) => { return Some(Ok(values)); },
                Ok(None) => {},
                Err(e) => { return Some(Err(e)); }
            }
        }
    }
}

pub struct NCDFlatSource {
    path: PathBuf,
    config: NCDFlatConfig
}

impl NCDFlatSource {
    pub fn new(path: &Path, config: &NCDFlatConfig) -> io::Result<NCDFlatSource> {
        Ok(NCDFlatSource { path: path.to_path_buf(), config: config.clone() })
    }
}

impl NCDValueSource for NCDFlatSource {
    fn iter<'a>(&'a self) -> io::Result<Box<dyn Iterator<Item=io::Result<(Vec<u8>,Vec<u8>)>> + 'a>> {
        Ok(Box::new(NCDFlatIterator::new(&self.path,&self.config)?))
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::{BufRead, BufReader}, path::{Path}};
    use crate::{StdNCDReadMutAccessor, build::{NCDBuild, NCDBuildConfig}, read::NCDReader, sources::flat::{NCDFlatConfig, NCDFlatSource}, test::{extract_all, temporary_path}, util::{NCDError, extract_whitespace, wrap_io_error}};

    fn do_test_flat() -> Result<(),NCDError> {
        let source_filename = Path::new(file!()).to_path_buf().parent().unwrap().join(Path::new("../../testdata/test_flat.txt"));
        let config = NCDFlatConfig::new();
        let source = wrap_io_error(NCDFlatSource::new(&Path::new(&source_filename),&config))?;
        let dest_path = wrap_io_error(temporary_path())?;
        let mut builder = NCDBuild::new(&NCDBuildConfig::new().target_page_size(16384).heap_wiggle_room(1.1).target_load_factor(0.75).rebuild_page_factor(1.1),&source,&dest_path)?;
        loop {
            println!("Attempting to build: {}",builder.describe_attempt());
            let success = builder.attempt()?;
            if success { break; }
            println!("  {}",builder.result());
        }
        drop(builder);
        let mut tmp_file = wrap_io_error(File::open(&dest_path))?;
        let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut tmp_file))?;
        let mut reader = NCDReader::new(std)?;
        let tmp_file = wrap_io_error(File::open(&source_filename))?;
        for line in BufReader::new(tmp_file).lines() {
            let line = wrap_io_error(line)?;
            let (key,value) = extract_whitespace(1,&line);
            let ncd_value = reader.lookup(&key.as_bytes())?; 
            assert!(ncd_value.is_some());
            let ncd_value = ncd_value.unwrap();
            assert_eq!(ncd_value.as_slice(),value.as_bytes());
        }
        Ok(())
    }

    fn do_test_flat_config(path: &str, config: &NCDFlatConfig, values: &[(&[u8],&[u8])], pass_in: bool) -> Result<(),NCDError> {
        let mut pass = true;
        let source = wrap_io_error(NCDFlatSource::new(&Path::new(path),config))?;
        let got_values = extract_all(&source)?;
        for (given,got) in values.iter().zip(got_values.iter()) {
            if given.0 != got.0 { pass = false; }
            if given.1 != got.1 { pass = false; }
        }
        if values.len() != got_values.len() { pass = false; }
        assert_eq!(pass,pass_in);
        Ok(())
    }

    #[test]
    fn test_flat() {
        do_test_flat().unwrap()
    }

    #[test]
    fn test_flat_config() {
        let simple_config = NCDFlatConfig::new();
        let notrim_config = simple_config.trim_tail(false);
        let comment_config = simple_config.comment_char(Some("//".to_string()));
        let inline_comment_config = comment_config.inline_comments(true);
        do_test_flat_config("testdata/test_flat.txt",&simple_config,&[(b"hello",b"world"),(b"x",b"y"),(b"a",b"b"),(b"123",b"456")],true).unwrap();
        do_test_flat_config("testdata/test_flat_comment.txt",&comment_config,&[(b"hello",b"world"),(b"123",b"456")],true).unwrap();
        do_test_flat_config("testdata/test_flat_comment.txt",&simple_config,&[(b"hello",b"world"),(b"123",b"456")],false).unwrap();
        do_test_flat_config("testdata/test_flat_comment.txt",&notrim_config,&[(b"hello",b"world   "),(b"123",b"456")],false).unwrap();
        do_test_flat_config("testdata/test_flat_comment_inline.txt",&inline_comment_config,&[(b"hello",b"world"),(b"123",b"456")],true).unwrap();
        do_test_flat_config("testdata/test_flat_comment_inline.txt",&comment_config,&[(b"hello",b"world"),(b"123",b"456")],false).unwrap();
    }
}
