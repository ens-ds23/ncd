use std::{ fs::{File, OpenOptions}, io::{self, Read, Seek, SeekFrom, Write}, path::Path};
use tempfile::tempfile;
use crate::{bitbash::{MAX_LESQLITE2_BYTES, compute_hash, lesqlite2_write, read_u64, read_uvar, write_bytes, write_u32, write_u64, write_uvar}, header::{HEADER_SIZE, NCDHeader }, util::{NCDError, wrap_io_error, write_blanks_to_file, write_zero_length_file}};

pub trait NCDValueSource {
    fn iter<'a>(&'a self) -> io::Result<Box<dyn Iterator<Item=io::Result<(Vec<u8>,Vec<u8>)>> + 'a>>;
}

const AUX_DATA_SIZE : usize = 8;

struct AuxData {
    heap_threshold: u64
}

struct AuxDataFile(File);

impl AuxDataFile {
    fn new(header: &NCDHeader) -> Result<AuxDataFile,NCDError> {
        let mut file = wrap_io_error(tempfile())?;
        write_blanks_to_file(&mut file,(AUX_DATA_SIZE as u64)*header.number_of_pages())?;
        Ok(AuxDataFile(file))
    }

    fn read(&mut self, index: u64) -> Result<AuxData,NCDError> {
        wrap_io_error(self.0.seek(SeekFrom::Start(index*(AUX_DATA_SIZE as u64))))?;
        let mut bytes = vec![0;AUX_DATA_SIZE];
        wrap_io_error(self.0.read_exact(&mut bytes))?;
        let mut offset = 0;
        let heap_threshold = read_u64(&bytes, &mut offset)?;
        Ok(AuxData { heap_threshold })
    }

    fn write(&mut self, index: u64, value: &AuxData) -> Result<(),NCDError> {
        wrap_io_error(self.0.seek(SeekFrom::Start(index*(AUX_DATA_SIZE as u64))))?;
        let mut bytes = vec![0;AUX_DATA_SIZE];
        let mut offset = 0;
        write_u64(&mut bytes,&mut offset,value.heap_threshold)?;
        wrap_io_error(self.0.write_all(&mut bytes))?;
        Ok(())
    }
}

pub struct NCDPageWriter {
    index: u64,
    aux: AuxData,
    threshold: u64
}

impl NCDPageWriter {
    fn new(aux_file: &mut AuxDataFile, index: u64, threshold: u64) -> Result<NCDPageWriter,NCDError> {
        let aux = aux_file.read(index)?;
        Ok(NCDPageWriter { index, aux, threshold })
    }

    fn heap_room(&self, header: &NCDHeader) -> u64 {
        (header.heap_size() as u64)-self.aux.heap_threshold
    }

    fn add_internal(&mut self, attempt: &mut NCDWriteAttempt, bytes: &[u8]) -> Result<u64,NCDError> {
        let space = self.heap_room(&attempt.header);
        if (space as usize) < bytes.len() {
            return Err(NCDError::HeapFull);
        }
        wrap_io_error(
            attempt.file.seek(SeekFrom::Start(&attempt.header.page_offset(self.index)+(self.aux.heap_threshold as u64)))
        )?;
        wrap_io_error(
            attempt.file.write_all(bytes)
        )?;
        let out = self.aux.heap_threshold;
        self.aux.heap_threshold += bytes.len() as u64;
        Ok(out)
    }

    fn add_external_bytes(&mut self, attempt: &mut NCDWriteAttempt, bytes: &[u8]) -> Result<u64,NCDError> {
        let offset = attempt.header.structured_size() + attempt.external_offset;
        wrap_io_error(
            attempt.file.seek(SeekFrom::Start(offset))
        )?;
        wrap_io_error(
            attempt.file.write_all(bytes)
        )?;
        let out = attempt.external_offset;
        attempt.external_offset += bytes.len() as u64;
        Ok(attempt.header.structured_size()+out)
    }

    fn make_external_pointer(&self, start: u64, size: u64, ext_hash: u32) -> Result<Vec<u8>,NCDError> {
        let mut bytes = vec![0;2*MAX_LESQLITE2_BYTES+4];
        let mut offset = 1;
        lesqlite2_write(&mut bytes, &mut offset, start)?;
        lesqlite2_write(&mut bytes, &mut offset, size)?;
        write_u32(&mut bytes, &mut offset, ext_hash)?;
        Ok(bytes[0..offset as usize].to_vec())
    }

    fn add_external(&mut self, attempt: &mut NCDWriteAttempt, ext_hash: u32, bytes: &[u8]) -> Result<u64,NCDError> {
        let offset = self.add_external_bytes(attempt,bytes)?;
        let pointer = self.make_external_pointer(offset,bytes.len() as u64,ext_hash)?;
        let space = self.heap_room(&attempt.header);
        if (space as usize) < pointer.len() {
            return Err(NCDError::HeapFull);
        }
        wrap_io_error(
            attempt.file.seek(SeekFrom::Start(attempt.header.page_offset(self.index)+(self.aux.heap_threshold as u64)))
        )?;
        wrap_io_error(
            attempt.file.write_all(&pointer)
        )?;
        let out = self.aux.heap_threshold;
        self.aux.heap_threshold += pointer.len() as u64;
        Ok(out)
    }

    fn add_data(&mut self, attempt: &mut NCDWriteAttempt, ext_hash: u32, bytes: &[u8]) -> Result<u64,NCDError> {
        if bytes.len() as u64 > self.threshold {
            self.add_external(attempt,ext_hash,bytes)
        } else {
            self.add_internal(attempt,bytes)
        }
    }

    fn write_hash(&mut self, header: &NCDHeader, file: &mut File, mut hash: u32, value: u64) -> Result<(),NCDError> {
        let plen = header.pointer_length() as u32;
        let first_hash = hash;
        wrap_io_error(
            file.seek(SeekFrom::Start(header.table_offset(self.index)))
        )?;
        let mut bytes = vec![0;(header.table_size_entries()*plen) as usize];
        wrap_io_error(file.read_exact(&mut bytes))?;
        let unused_value = header.unused_value()?;
        loop {
            let entry_offset = (hash*plen) as usize;
            let mut offset = entry_offset;
            let entry = read_uvar(&bytes, &mut offset,plen as usize)?;
            if entry == unused_value {
                let mut offset = entry_offset;
                write_uvar(&mut bytes, &mut offset, value,plen as usize)?;
                wrap_io_error(
                    file.seek(SeekFrom::Start(header.table_offset(self.index)+(hash*plen) as u64))
                )?;
                wrap_io_error(
                    file.write_all(&bytes[entry_offset..entry_offset+(plen as usize)])
                )?;
                return Ok(());
            }
            hash = (hash+1) % header.table_size_entries();
            if hash == first_hash { return Err(NCDError::TableFull); }
        }
    }

    fn add(&mut self, attempt: &mut NCDWriteAttempt, slot_hash: u32, ext_hash: u32, bytes: &[u8]) -> Result<(),NCDError> {
        let offset = self.add_data(attempt,ext_hash,bytes)?;
        self.write_hash(&attempt.header,&mut attempt.file,slot_hash,offset)?;
        attempt.aux.write(self.index,&self.aux)?;
        Ok(())
    }
}

pub(crate) struct NCDWriteAttempt<'a> {
    header: &'a NCDHeader,
    file: File,
    aux: AuxDataFile,
    external_offset: u64,
    threshold: u64
}

fn write_blank_tables(header: &NCDHeader, file: &mut File) -> Result<(),NCDError> {
    let mut unset = vec![0xFF_u8;(header.pointer_length() as u32 * header.table_size_entries()) as usize + 4];
    let mut offset = unset.len()-4;
    write_u32(&mut unset,&mut offset,header.stamp())?;
    for page_num in 0..header.number_of_pages() {
        wrap_io_error(file.seek(SeekFrom::Start(header.table_offset(page_num))))?;
        wrap_io_error(file.write_all(&mut unset))?;
    }
    Ok(())
}

fn prepare_output_file(header: &NCDHeader, path: &Path) -> Result<File,NCDError> {
    let mut file = wrap_io_error(OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path))?;
    let size = header.structured_size();
    write_blanks_to_file(&mut file,size)?;
    header.write(&mut file)?;
    write_blank_tables(&header,&mut file)?;
    Ok(file)
}

impl<'a> NCDWriteAttempt<'a> {
    pub fn new(header: &'a NCDHeader, path: &Path, threshold: u64) -> Result<NCDWriteAttempt<'a>,NCDError> {
        wrap_io_error(write_zero_length_file(path))?;
        let file = prepare_output_file(&header,path)?;
        let mut aux_file = AuxDataFile::new(&header)?;
        let first_page = AuxData {
            heap_threshold: HEADER_SIZE as u64
        };
        aux_file.write(0,&first_page)?;
        Ok(NCDWriteAttempt { header, file, aux: aux_file, external_offset: 0, threshold })
    }

    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(),NCDError> {
        let hash = compute_hash(key)?;
        let page_hash = self.header.hash_page_index(hash);
        let mut bytes = vec![0;key.len()+value.len()+2*MAX_LESQLITE2_BYTES];
        let mut start = 0;
        lesqlite2_write(&mut bytes,&mut start,key.len() as u64+1)?;
        write_bytes(&mut bytes, &mut start, key)?;
        lesqlite2_write(&mut bytes,&mut start,value.len() as u64)?;
        write_bytes(&mut bytes, &mut start, value)?;
        let bytes = &bytes[0..start];
        let mut page_writer = NCDPageWriter::new(&mut self.aux,page_hash,self.threshold)?;
        let slot_hash = self.header.hash_page_slot(hash);
        let ext_hash = self.header.hash_ext(hash);
        page_writer.add(self,slot_hash,ext_hash,bytes)?;
        Ok(())
    }

    pub fn add_all(&mut self, source: &dyn NCDValueSource) -> Result<(),NCDError> {
        for key_value in wrap_io_error(source.iter())? {
            let (key,value) = wrap_io_error(key_value)?;
            self.add(&key,&value)?;
        }
        wrap_io_error(self.file.flush())?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{env::temp_dir, fs::File, path::Path};

    use crate::{StdNCDReadMutAccessor, header::NCDHeader, read::{NCDFileReader }, sources::hashmap::NCDHashMapValueSource, test::{ numeric_key_values}, util::{NCDError, wrap_io_error}};

    use super::{NCDWriteAttempt};

    const COUNT : u32 = 1000;

    fn do_file_write_smoke() -> Result<(),NCDError> {
        for size in &[None,Some(2),Some(4)] {
            let tmp_dir = temp_dir();
            let tmp_filename = Path::new(&tmp_dir).join("test2.ncd");
            let header = NCDHeader::new(64,512-64,64,*size,0x12345678)?;
            let mut writer = NCDWriteAttempt::new(&header,&tmp_filename,10)?;
            let source = NCDHashMapValueSource::new(numeric_key_values(COUNT));
            writer.add_all(&source)?;
            drop(writer);
            /**/
            let mut file = wrap_io_error(File::open(tmp_filename))?;
            let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file))?;
            let mut reader = NCDFileReader::new(std)?;
            let kv = numeric_key_values(COUNT);
            let mut k_sorted = kv.keys().collect::<Vec<_>>();
            k_sorted.sort();
            for k in k_sorted.iter() {
                let v = kv.get(*k).unwrap().to_vec();
                let value = reader.lookup(&k)?;
                assert_eq!(Some(v),value);
            }
        }
        Ok(())
    }

    #[test]
    fn file_write_smoke() {
        do_file_write_smoke().unwrap();
    }

    fn do_file_write_big_zero_smoke() -> Result<(),NCDError> {
        for size in &[None,Some(2),Some(4)] {
            let tmp_dir = temp_dir();
            let tmp_filename = Path::new(&tmp_dir).join("test4.ncd");
            let header = NCDHeader::new(64,512-64,64,*size,0x12345678)?;
            let mut writer = NCDWriteAttempt::new(&header,&tmp_filename,10)?;
            let source = NCDHashMapValueSource::new(numeric_key_values(0));
            writer.add_all(&source)?;
            drop(writer);
            /**/
            let mut file = wrap_io_error(File::open(tmp_filename))?;
            let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file))?;
            let mut reader = NCDFileReader::new(std)?;
            let kv = numeric_key_values(COUNT);
            for k in kv.keys() {
                let value = reader.lookup(k)?;
                assert_eq!(None,value);
            }
        }

        Ok(())
    }

    fn do_file_write_small_zero_smoke() -> Result<(),NCDError> {
        for size in &[None,Some(2),Some(4)] {
            let tmp_dir = temp_dir();
            let tmp_filename = Path::new(&tmp_dir).join("test5.ncd");
            let header = NCDHeader::new(0,0,0,*size,0x12345678)?;
            let mut writer = NCDWriteAttempt::new(&header,&tmp_filename,10)?;
            let source = NCDHashMapValueSource::new(numeric_key_values(0));
            writer.add_all(&source)?;
            drop(writer);
            /**/
            let mut file = wrap_io_error(File::open(tmp_filename))?;
            let std = wrap_io_error(StdNCDReadMutAccessor::new(&mut file))?;
            let mut reader = NCDFileReader::new(std)?;
            let kv = numeric_key_values(COUNT);
            for k in kv.keys() {
                let value = reader.lookup(k)?;
                assert_eq!(None,value);
            }
        }
        Ok(())
    }

    #[test]
    fn file_write_big_zero_smoke() {
        do_file_write_big_zero_smoke().unwrap();
    }

    #[test]
    fn file_write_small_zero_smoke() {
        do_file_write_small_zero_smoke().unwrap();
    }
}
