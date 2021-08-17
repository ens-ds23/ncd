use std::io::{BufReader, Cursor};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use murmur3::murmur3_x64_128;
use crate::util::{NCDError, wrap_io_error};

pub const MAX_LESQLITE2_BYTES : usize = 9;

pub(crate) fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32,NCDError> {
    if bytes.len() < *offset+4 { return Err(NCDError::CorruptNCDFile(format!("premature EOF"))); }
    let out =  wrap_io_error((&bytes[*offset..]).read_u32::<LittleEndian>());
    *offset += 4;
    out
}

pub(crate) fn read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16,NCDError> {
    if bytes.len() < *offset+2 { return Err(NCDError::CorruptNCDFile(format!("premature EOF"))); }
    let out =  wrap_io_error((&bytes[*offset..]).read_u16::<LittleEndian>());
    *offset += 2;
    out
}

pub(crate) fn write_u32(bytes: &mut [u8], offset: &mut usize, value: u32) -> Result<(),NCDError> {
    let mut cursor = Cursor::new(bytes);
    cursor.set_position(*offset as u64);
    wrap_io_error(cursor.write_u32::<LittleEndian>(value))?;
    *offset += 4;
    Ok(())
}

pub(crate) fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64,NCDError> {
    if bytes.len() < *offset+8 { return Err(NCDError::CorruptNCDFile(format!("premature EOF"))); }
     let out = wrap_io_error((&bytes[*offset..]).read_u64::<LittleEndian>());
     *offset += 8;
     out
}

pub(crate) fn read_uvar(bytes: &[u8], offset: &mut usize, len: usize) -> Result<u64,NCDError> {
    match len {
        8 => read_u64(bytes,offset),
        4 => read_u32(bytes,offset).map(|v| v as u64),
        2 => read_u16(bytes,offset).map(|v| v as u64),
        _ => Err(NCDError::CorruptNCDFile(format!("bad pointer length {}",len)))
    }
}

pub(crate) fn all_set(len: usize) -> Result<u64,NCDError> {
    match len {
        8 => Ok(0xFFFFFFFFFFFFFFFF),
        4 => Ok(0xFFFFFFFF),
        2 => Ok(0xFFFF),
        _ => Err(NCDError::CorruptNCDFile(format!("bad pointer length {}",len)))
    }
}

pub(crate) fn write_u64(bytes: &mut [u8], offset: &mut usize, value: u64) -> Result<(),NCDError> {
    let mut cursor = Cursor::new(bytes);
    cursor.set_position(*offset as u64);
    wrap_io_error(cursor.write_u64::<LittleEndian>(value))?;
    *offset += 8;
    Ok(())
}

pub(crate) fn write_u16(bytes: &mut [u8], offset: &mut usize, value: u16) -> Result<(),NCDError> {
    let mut cursor = Cursor::new(bytes);
    cursor.set_position(*offset as u64);
    wrap_io_error(cursor.write_u16::<LittleEndian>(value))?;
    *offset += 2;
    Ok(())
}

pub(crate) fn write_uvar(bytes: &mut [u8], offset: &mut usize, value: u64, len: usize) -> Result<(),NCDError> {
    match len {
        8 => write_u64(bytes,offset,value),
        4 => write_u32(bytes,offset,value as u32),
        2 => write_u16(bytes,offset,value as u16),
        _ => Err(NCDError::CorruptNCDFile(format!("bad pointer length {}",len)))
    }
}

pub(crate) fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8],NCDError> {
    if bytes.len() < *offset+len { return Err(NCDError::CorruptNCDFile(format!("premature EOF"))); }
     let out = &bytes[*offset..(*offset+len)];
     *offset += len;
     Ok(out)
}

pub(crate) fn write_bytes<'a>(bytes: &'a mut [u8], offset: &mut usize, value: &[u8]) -> Result<(),NCDError> {
    if bytes.len() < *offset+value.len() { return Err(NCDError::CorruptNCDFile(format!("premature EOF"))); }
    bytes[*offset..(*offset+value.len())].clone_from_slice(value);
    *offset += value.len();
    Ok(())
}

pub(crate) fn lesqlite2_read(data: &[u8], start: &mut usize) -> Result<u64,NCDError> {
    if data.len() < *start { return Err(NCDError::CorruptNCDFile(format!("bad lesqlite2 integer"))); }
    let len = data.len()-*start;
    let data = &data[*start..];
    if len < 1 { return Err(NCDError::CorruptNCDFile(format!("bad lesqlite2 integer"))); }
    let b0 = data[0] as u64;
    if b0 < 178 {
        *start += 1;
        return Ok(b0);
    }
    if len < 2 { return Err(NCDError::CorruptNCDFile(format!("bad lesqlite2 integer"))); }
    let b1 = data[1] as u64;
    if b0 < 242 {
        *start += 2;
        return Ok(178 + ((b0-178)<<8) + b1);
    }
    if len < 3 { return Err(NCDError::CorruptNCDFile(format!("bad lesqlite2 integer"))); }
    let b2 = data[2] as u64;
    if b0 < 250 {
        *start += 3;
        return Ok(16562 + ((b0-242)<<16) + (b1<<8) + b2);
    }
    let n = (b0-247) as usize;
    if len <= n { return Err(NCDError::CorruptNCDFile(format!("bad lesqlite2 iteger"))); }
    let mut v = 0;
    let mut shift = 0;
    for i in 0..n {
        v += (data[i+1] as u64) << shift;
        shift += 8;
    }
    *start += n+1;
    Ok(v)
}

const C0 : u64 = 178;
const C1 : u64 = 16562;
const C2: u64 = 540850;
pub(crate) fn lesqlite2_write(data: &mut[u8], start: &mut usize, mut value: u64) -> Result<(),NCDError> {
    let len = data.len()-*start;
    if value < C0 {
        if len < 1 { return Err(NCDError::CorruptNCDFile(format!("buffer too small"))); }
        data[*start] = value as u8;
        *start += 1;
    } else if value < C1 {
        // 178 + ((B0-178) << 8) + B[1]
        if len < 2 { return Err(NCDError::CorruptNCDFile(format!("buffer too small"))); }
        data[*start] = ((value-C0) >> 8) as u8 + C0 as u8;
        data[*start+1] = ((value-C0) & 0xFF) as u8;
        *start += 2;
    } else if value <  C2 {
        // 16562 + ((B0-242) << 16) + B[1..2]
        if len < 3 { return Err(NCDError::CorruptNCDFile(format!("buffer too small"))); }
        data[*start] = ((value-C1) >> 16) as u8 + 242;
        data[*start+1] = (((value-C1) & 0xFF00) >> 8) as u8;
        data[*start+2] = (((value-C1) & 0xFF)) as u8;
        *start += 3;
    } else {
        if len < 1 { return Err(NCDError::CorruptNCDFile(format!("buffer too small"))); }
        let bits_used = 64 - value.leading_zeros();
        let bytes_used = (bits_used+7)/8;
        data[*start] = bytes_used as u8 + 247;
        *start += 1;
        if len < bytes_used as usize+1 { return Err(NCDError::CorruptNCDFile(format!("buffer too small"))); }
        for _ in 0..bytes_used {
            data[*start] = (value & 0xFF) as u8;
            value >>= 8;
            *start += 1;
        }
    }
    Ok(())
}

pub(crate) fn bounds_check(heap: &[u8], offset: usize, length: usize) -> Result<(),NCDError> {
    if heap.len() < offset+length {
        return Err(NCDError::CorruptNCDFile(format!("bad heap reference")));
    } else {
        return Ok(())
    }
}

pub(crate) fn compute_hash(key: &[u8]) -> Result<u64,NCDError> {
    let mut hash_key = BufReader::new(key);
    let value = wrap_io_error(murmur3_x64_128(&mut hash_key,0))?;
    Ok((value >> 64) as u64)
}

#[cfg(test)]
mod test {
    use crate::{bitbash::{MAX_LESQLITE2_BYTES, all_set, bounds_check, compute_hash, lesqlite2_read, lesqlite2_write, read_bytes, read_u16, read_u32, read_u64, read_uvar, write_bytes, write_u16, write_u32, write_u64, write_uvar}, util::NCDError};

    fn do_test_hash() -> Result<(),NCDError> {
        assert_eq!(0x5b1e906a48ae1d19,compute_hash(b"hello")?);
        assert_eq!(0,compute_hash(b"")?);
        Ok(())
    }
 
    #[test] fn test_hash() { do_test_hash().unwrap() }
 
    #[test]
    fn test_bounds_check() -> Result<(),NCDError> {
        let heap = vec![0;123];
        assert!(bounds_check(&heap,0,123).is_ok());
        assert!(bounds_check(&heap,1,123).is_err());
        assert!(bounds_check(&heap,0,124).is_err());
        let heap = vec![];
        assert!(bounds_check(&heap,0,0).is_ok());
        assert!(bounds_check(&heap,0,1).is_err());
        Ok(())
    }

    fn test_lesqlite2_value(bytes: &mut [u8],value: u64) -> Result<(),NCDError> {
        let mut start = 0;
        lesqlite2_write(bytes,&mut start,value)?;
        let mut start = 0;
        let out = lesqlite2_read(bytes, &mut start)?;
        assert_eq!(value,out);
        Ok(())
    }

    fn do_test_lesqlite2() -> Result<(),NCDError> {
        let mut bytes = vec![0;MAX_LESQLITE2_BYTES];
        for i in 0..1000000 {
            test_lesqlite2_value(&mut bytes,i)?;
        }
        for bits in 6..60 {
            for wiggle in 0..9 {
                let value = (1<<bits) + wiggle - 4;
                test_lesqlite2_value(&mut bytes,value)?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_lesqlite2() {
        do_test_lesqlite2().unwrap();
    }

    fn do_test_read_write_bytes() -> Result<(),NCDError> {
        let mut bytes = vec![0;20];
        let mut offset = 0;
        write_bytes(&mut bytes,&mut offset,&[0,1,2,3,4,5,6,7,8,9])?;
        for i in 0..10 {
            assert_eq!(i as u8,bytes[i]);
        }
        let mut offset = 0;
        let cmp = read_bytes(&mut bytes,&mut offset,10)?;
        assert_eq!(10,cmp.len());
        for i in 0..10 {
            assert_eq!(i as u8,cmp[i]);
        }
        write_bytes(&mut bytes,&mut offset,&[10,11,12,13])?;
        let mut offset = 0;
        let cmp = read_bytes(&mut bytes,&mut offset,14)?;
        assert_eq!(14,cmp.len());
        for i in 0..14 {
            assert_eq!(i as u8,cmp[i]);
        }
        assert_eq!(14,offset);
        let mut offset = 4;
        write_bytes(&mut bytes,&mut offset,&[])?;
        for i in 0..10 {
            assert_eq!(i as u8,bytes[i]);
        }
        Ok(())
    }

    #[test]
    fn test_read_write_bytes() {
        do_test_read_write_bytes().unwrap();
    }

    fn do_test_read_write(size: usize) -> Result<(),NCDError> {
        let mut bytes = vec![0;8];
        let mut offset = 0;
        let value = 0x123456789ABCDEF;
        write_uvar(&mut bytes,&mut offset,value,size)?;
        assert_eq!(size,offset);
        for b in 0..8 {
            let cmp = ((value >> (b*8)) & 0xFF) as u8;
            if b < size {
                assert_eq!(cmp,bytes[b]);
            } else {
                assert_eq!(0,bytes[b]);
            }
        }
        let mut offset = 0;
        let v = read_uvar(&bytes,&mut offset,size)?;
        assert_eq!(value & all_set(size)?,v);
        Ok(())
    }

    #[test]
    fn test_read_write() {
        for size in [2,4,8] {
            do_test_read_write(size).unwrap();
        }
    }

    #[test]
    fn test_premature_eof_read() {
        const X : usize = 6;

        for b in 0..8 {
            let mut bytes = vec![0;b+X];
            let mut offset = X;
            assert_eq!(b==8,read_u64(&mut bytes, &mut offset).is_ok());
        }
        for b in 0..4 {
            let mut bytes = vec![0;b+X];
            let mut offset = X;
            assert_eq!(b==4,read_u32(&mut bytes, &mut offset).is_ok());
        }
        for b in 0..2 {
            let mut bytes = vec![0;b+X];
            let mut offset = X;
            assert_eq!(b==2,read_u16(&mut bytes, &mut offset).is_ok());
        }
        let mut bytes = vec![0;9+X];
        let mut offset = X;
        assert!(read_bytes(&mut bytes,&mut offset,9).is_ok());
        assert!(read_bytes(&mut bytes,&mut offset,10).is_err());
    }

    #[test]
    fn test_premature_eof_write() {
        const X : usize = 6;

        for b in 0..8 {
            let mut bytes = vec![0;b+X];
            let mut offset = X;
            assert_eq!(b==8,write_u64(&mut bytes, &mut offset,0x0123456789ABCDEF).is_ok());
        }
        for b in 0..4 {
            let mut bytes = vec![0;b+X];
            let mut offset = X;
            assert_eq!(b==4,write_u32(&mut bytes, &mut offset,0x01234567).is_ok());
        }
        for b in 0..2 {
            let mut bytes = vec![0;b+X];
            let mut offset = X;
            assert_eq!(b==2,write_u16(&mut bytes, &mut offset,0x0123).is_ok());
        }
        let mut bytes = vec![0;9+X];
        let mut offset = X;
        assert!(write_bytes(&mut bytes,&mut offset,&[0,1,2,3,4,5,6,7,8]).is_ok());
        assert!(write_bytes(&mut bytes,&mut offset,&[0,1,2,3,4,5,6,7,8,9]).is_err());
    }
}
