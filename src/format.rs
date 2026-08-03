use crate::{Error, Result};

pub(crate) const FILE_HEADER_LEN: usize = 64;
pub(crate) const TX_HEADER_LEN: usize = 40;
pub(crate) const TX_FOOTER_LEN: usize = 24;
pub(crate) const RECORD_HEADER_LEN: usize = 16;

const FILE_MAGIC: &[u8; 8] = b"INSPACE\0";
const TX_MAGIC: &[u8; 8] = b"INSTXN01";
const END_MAGIC: &[u8; 8] = b"INSEND01";
const VERSION: u32 = 1;

pub(crate) const CREATE_BUCKET: u8 = 1;
pub(crate) const DELETE_BUCKET: u8 = 2;
pub(crate) const PUT: u8 = 3;
pub(crate) const DELETE: u8 = 4;

pub(crate) type Operation = (u8, Vec<u8>, Vec<u8>, Vec<u8>);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Record {
    pub kind: u8,
    pub bucket_offset: usize,
    pub bucket_len: usize,
    pub key_offset: usize,
    pub key_len: usize,
    pub value_offset: usize,
    pub value_len: usize,
}

#[derive(Debug)]
pub(crate) struct Transaction {
    pub txid: u64,
    pub records: Vec<Record>,
    pub end: usize,
}

pub(crate) fn file_header() -> [u8; FILE_HEADER_LEN] {
    let mut header = [0_u8; FILE_HEADER_LEN];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(FILE_HEADER_LEN as u32).to_le_bytes());
    header
}

pub(crate) fn validate_file_header(bytes: &[u8]) -> Result<()> {
    if bytes.len() < FILE_HEADER_LEN {
        return Err(Error::Corrupt("file header is truncated"));
    }
    if &bytes[..8] != FILE_MAGIC {
        return Err(Error::Corrupt("invalid file magic"));
    }
    if read_u32(bytes, 8)? != VERSION {
        return Err(Error::Corrupt("unsupported format version"));
    }
    if read_u32(bytes, 12)? as usize != FILE_HEADER_LEN {
        return Err(Error::Corrupt("invalid file header length"));
    }
    Ok(())
}

pub(crate) fn encode_transaction(txid: u64, operations: &[Operation]) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    for (kind, bucket, key, value) in operations {
        let bucket_len = u32::try_from(bucket.len()).map_err(|_| Error::TooLarge)?;
        let key_len = u32::try_from(key.len()).map_err(|_| Error::TooLarge)?;
        let value_len = u32::try_from(value.len()).map_err(|_| Error::TooLarge)?;
        payload.push(*kind);
        payload.extend_from_slice(&[0; 3]);
        payload.extend_from_slice(&bucket_len.to_le_bytes());
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&value_len.to_le_bytes());
        payload.extend_from_slice(bucket);
        payload.extend_from_slice(key);
        payload.extend_from_slice(value);
    }

    let payload_len = u64::try_from(payload.len()).map_err(|_| Error::TooLarge)?;
    let record_count = u32::try_from(operations.len()).map_err(|_| Error::TooLarge)?;
    let checksum = checksum(&payload);
    let mut bytes = Vec::with_capacity(TX_HEADER_LEN + payload.len() + TX_FOOTER_LEN);
    bytes.extend_from_slice(TX_MAGIC);
    bytes.extend_from_slice(&(TX_HEADER_LEN as u32).to_le_bytes());
    bytes.extend_from_slice(&record_count.to_le_bytes());
    bytes.extend_from_slice(&txid.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(END_MAGIC);
    bytes.extend_from_slice(&txid.to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

/// Returns `Ok(None)` only for an incomplete transaction at the end of a file.
pub(crate) fn parse_transaction(bytes: &[u8], start: usize) -> Result<Option<Transaction>> {
    let Some(header_end) = start.checked_add(TX_HEADER_LEN) else {
        return Err(Error::Corrupt("transaction offset overflow"));
    };
    if header_end > bytes.len() {
        return Ok(None);
    }
    if &bytes[start..start + 8] != TX_MAGIC {
        return Err(Error::Corrupt("invalid transaction magic"));
    }
    if read_u32(bytes, start + 8)? as usize != TX_HEADER_LEN {
        return Err(Error::Corrupt("invalid transaction header length"));
    }
    let record_count = read_u32(bytes, start + 12)? as usize;
    let txid = read_u64(bytes, start + 16)?;
    let payload_len = usize::try_from(read_u64(bytes, start + 24)?)
        .map_err(|_| Error::Corrupt("transaction is too large"))?;
    let expected_checksum = read_u64(bytes, start + 32)?;
    let payload_end = header_end
        .checked_add(payload_len)
        .ok_or(Error::Corrupt("transaction length overflow"))?;
    let end = payload_end
        .checked_add(TX_FOOTER_LEN)
        .ok_or(Error::Corrupt("transaction length overflow"))?;
    if end > bytes.len() {
        return Ok(None);
    }
    let payload = &bytes[header_end..payload_end];
    if checksum(payload) != expected_checksum {
        return Err(Error::Corrupt("transaction checksum mismatch"));
    }
    if &bytes[payload_end..payload_end + 8] != END_MAGIC
        || read_u64(bytes, payload_end + 8)? != txid
        || read_u64(bytes, payload_end + 16)? != expected_checksum
    {
        return Err(Error::Corrupt("invalid transaction footer"));
    }

    let mut cursor = header_end;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let record_header_end = cursor
            .checked_add(RECORD_HEADER_LEN)
            .ok_or(Error::Corrupt("record offset overflow"))?;
        if record_header_end > payload_end {
            return Err(Error::Corrupt("record header exceeds transaction"));
        }
        let kind = bytes[cursor];
        if !matches!(kind, CREATE_BUCKET | DELETE_BUCKET | PUT | DELETE) {
            return Err(Error::Corrupt("unknown record kind"));
        }
        let bucket_len = read_u32(bytes, cursor + 4)? as usize;
        let key_len = read_u32(bytes, cursor + 8)? as usize;
        let value_len = read_u32(bytes, cursor + 12)? as usize;
        let bucket_offset = record_header_end;
        let key_offset = bucket_offset
            .checked_add(bucket_len)
            .ok_or(Error::Corrupt("record length overflow"))?;
        let value_offset = key_offset
            .checked_add(key_len)
            .ok_or(Error::Corrupt("record length overflow"))?;
        cursor = value_offset
            .checked_add(value_len)
            .ok_or(Error::Corrupt("record length overflow"))?;
        if cursor > payload_end {
            return Err(Error::Corrupt("record exceeds transaction"));
        }
        records.push(Record {
            kind,
            bucket_offset,
            bucket_len,
            key_offset,
            key_len,
            value_offset,
            value_len,
        });
    }
    if cursor != payload_end {
        return Err(Error::Corrupt("transaction has unclaimed payload bytes"));
    }
    Ok(Some(Transaction { txid, records, end }))
}

pub(crate) fn checksum(bytes: &[u8]) -> u64 {
    // FNV-1a is deliberately simple and fast. It detects torn/corrupt writes; it
    // is not intended to provide cryptographic authenticity.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let raw = bytes
        .get(offset..offset + 4)
        .ok_or(Error::Corrupt("unexpected end of file"))?;
    Ok(u32::from_le_bytes(raw.try_into().expect("length checked")))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let raw = bytes
        .get(offset..offset + 8)
        .ok_or(Error::Corrupt("unexpected end of file"))?;
    Ok(u64::from_le_bytes(raw.try_into().expect("length checked")))
}
