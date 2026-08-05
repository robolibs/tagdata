use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Seek, SeekFrom},
    mem::{offset_of, size_of},
    path::Path,
};

use crate::{
    Error, Result,
    db::{FORMAT_VERSION, MAGIC_VALUE},
    meta::Meta,
    page::Page,
};

const MAX_BOOTSTRAP_SCAN: u64 = 16 * 1024 * 1024;

/// Persisted format facts discoverable without mapping the complete database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormatInfo {
    pub page_size: u64,
    pub version: u32,
    pub transaction_id: u64,
    pub metadata_page: u32,
    pub file_bytes: u64,
}

impl FormatInfo {
    pub fn inspect(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_bytes = file.metadata()?.len();
        let header = offset_of!(Page, ptr) + size_of::<Meta>();
        let primary_len = header.min(usize::try_from(file_bytes).unwrap_or(usize::MAX));
        let mut primary = vec![0; primary_len];
        file.read_exact(&mut primary)?;
        // Skip the bounded recovery scan only when both redundant metadata
        // pages authenticate under the page size declared by page zero.
        if let Some(page_size) = metadata_page_size(&primary, 0, 0)
            && let Ok((info, 2)) = inspect_candidate(&mut file, file_bytes, page_size)
        {
            return Ok(info);
        }

        let scan_len = file_bytes.min(MAX_BOOTSTRAP_SCAN);
        let scan_len = usize::try_from(scan_len)
            .map_err(|_| Error::InvalidDB("bootstrap scan is too large".into()))?;
        let mut prefix = vec![0; scan_len];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut prefix)?;

        let mut candidates = BTreeSet::new();
        if let Some(page_size) = metadata_page_size(&prefix, 0, 0) {
            candidates.insert(page_size);
        }
        let minimum = 1024_usize;
        let header = size_of::<Page>() + size_of::<Meta>();
        for offset in minimum..prefix.len().saturating_sub(header) {
            if let Some(page_size) = metadata_page_size(&prefix, offset, 1)
                && page_size == offset as u64
            {
                candidates.insert(page_size);
            }
        }

        let mut valid = Vec::new();
        for page_size in candidates {
            if let Ok((info, _)) = inspect_candidate(&mut file, file_bytes, page_size) {
                valid.push(info);
            }
        }
        match valid.len() {
            1 => Ok(valid.remove(0)),
            0 => Err(Error::InvalidDB(
                "could not identify a valid metadata page size".into(),
            )),
            _ => Err(Error::InvalidDB(
                "ambiguous metadata page sizes; use the forensic override".into(),
            )),
        }
    }
}

fn metadata_page_size(bytes: &[u8], offset: usize, expected_id: u64) -> Option<u64> {
    let header_end = offset.checked_add(size_of::<Page>())?;
    let meta_end = offset.checked_add(offset_of!(Page, ptr) + size_of::<Meta>())?;
    if header_end > bytes.len() || meta_end > bytes.len() {
        return None;
    }
    if read_u64(bytes, offset)? != expected_id
        || *bytes.get(offset + offset_of!(Page, page_type))? != Page::TYPE_META
    {
        return None;
    }
    let meta = offset + offset_of!(Page, ptr);
    if read_u32(bytes, meta + offset_of!(Meta, magic))? != MAGIC_VALUE {
        return None;
    }
    let page_size = read_u64(bytes, meta + offset_of!(Meta, pagesize))?;
    (1024..=MAX_BOOTSTRAP_SCAN)
        .contains(&page_size)
        .then_some(page_size)
}

fn inspect_candidate(
    file: &mut File,
    file_bytes: u64,
    page_size: u64,
) -> Result<(FormatInfo, usize)> {
    let bootstrap_len = page_size
        .checked_mul(2)
        .ok_or_else(|| Error::InvalidDB("bootstrap length overflow".into()))?;
    if bootstrap_len > file_bytes {
        return Err(Error::InvalidDB("metadata pages exceed the file".into()));
    }
    let mut bytes = vec![0; bootstrap_len as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut bytes)?;

    let mut valid = Vec::new();
    for id in 0..=1_u64 {
        let Ok(page) = Page::validate_block(&bytes, id, page_size) else {
            continue;
        };
        let current = page.meta();
        if current.valid()
            && current.magic == MAGIC_VALUE
            && current.version == FORMAT_VERSION
            && current.pagesize == page_size
        {
            valid.push(current.clone());
        }
    }
    let valid_pages = valid.len();
    let meta = valid
        .into_iter()
        .max_by_key(|meta: &Meta| meta.tx_id)
        .ok_or_else(|| Error::InvalidDB("no valid metadata pages".into()))?;
    Ok((
        FormatInfo {
            page_size,
            version: meta.version,
            transaction_id: meta.tx_id,
            metadata_page: meta.meta_page,
            file_bytes,
        },
        valid_pages,
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_ne_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_ne_bytes(
        bytes.get(offset..offset + 8)?.try_into().ok()?,
    ))
}
