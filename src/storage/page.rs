use std::{
    io::Write,
    mem::size_of,
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::Arc,
};

use memmap2::Mmap;
use sha3::{Digest, Sha3_256};

use crate::{
    errors::{Error, Result},
    freelist::RetiredPage,
    meta::Meta,
    node::{Node, NodeData, NodeType},
};

pub(crate) type PageID = u64;

pub(crate) type PageType = u8;

pub(crate) const CHECKSUM_SIZE: usize = 32;

#[derive(Clone)]
pub(crate) struct Pages {
    pub(crate) data: Arc<Mmap>,
    pub(crate) pagesize: u64,
}

impl Pages {
    pub fn new(data: Arc<Mmap>, pagesize: u64) -> Pages {
        Pages { data, pagesize }
    }

    #[inline]
    pub fn page<'a>(&self, id: PageID) -> &'a Page {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &*(&self.data[(id * self.pagesize) as usize] as *const u8 as *const Page)
        }
    }

    pub(crate) fn validate(&self, id: PageID) -> Result<&Page> {
        Page::validate_block(&self.data, id, self.pagesize)
    }
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct Page {
    // id * pagesize is the offset from the beginning of the file
    pub(crate) id: PageID,
    pub(crate) page_type: PageType,
    // Number of elements on this page, the type of element depends on the pageType
    pub(crate) count: u64,
    // Number of additional pages after this one that are part of this block
    pub(crate) overflow: u64,
    // ptr serves as a reference to where the actual data starts
    pub(crate) ptr: u64,
}

impl Page {
    pub(crate) const TYPE_BRANCH: PageType = 0x01;
    pub(crate) const TYPE_LEAF: PageType = 0x02;
    pub(crate) const TYPE_META: PageType = 0x03;
    pub(crate) const TYPE_FREELIST: PageType = 0x04;

    #[inline]
    pub(crate) fn from_buf(buf: &[u8], id: PageID, pagesize: u64) -> &Page {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &*(&buf[(id * pagesize) as usize] as *const u8 as *const Page)
        }
    }

    pub(crate) fn validate_block(buf: &[u8], id: PageID, pagesize: u64) -> Result<&Page> {
        let offset = id
            .checked_mul(pagesize)
            .ok_or_else(|| Error::InvalidDB(format!("page {id} offset overflow")))?;
        let offset = checked_usize(id, offset, "offset")?;
        let header_end = offset
            .checked_add(size_of::<Page>())
            .ok_or_else(|| Error::InvalidDB(format!("page {id} header overflow")))?;
        if header_end > buf.len() {
            return Err(Error::InvalidDB(format!(
                "page {id} is outside the mapped file"
            )));
        }

        let page = Self::from_buf(buf, id, pagesize);
        if page.id != id {
            return Err(Error::InvalidDB(format!(
                "page {id} contains page id {}",
                page.id
            )));
        }
        if !matches!(
            page.page_type,
            Self::TYPE_BRANCH | Self::TYPE_LEAF | Self::TYPE_META | Self::TYPE_FREELIST
        ) {
            return Err(Error::InvalidDB(format!(
                "page {id} has invalid type {}",
                page.page_type
            )));
        }
        let block_pages = page
            .overflow
            .checked_add(1)
            .ok_or_else(|| Error::InvalidDB(format!("page {id} overflow count overflow")))?;
        let block_len = block_pages
            .checked_mul(pagesize)
            .ok_or_else(|| Error::InvalidDB(format!("page {id} block length overflow")))?;
        let block_len = checked_usize(id, block_len, "block length")?;
        let block_end = offset
            .checked_add(block_len)
            .ok_or_else(|| Error::InvalidDB(format!("page {id} block end overflow")))?;
        if block_end > buf.len() {
            return Err(Error::InvalidDB(format!(
                "page {id} overflow block exceeds the file"
            )));
        }

        if block_len < size_of::<Page>() + CHECKSUM_SIZE {
            return Err(Error::InvalidDB(format!("page {id} block is too small")));
        }
        verify_checksum(&buf[offset..block_end], id)?;
        page.validate_layout(block_len - CHECKSUM_SIZE)?;
        Ok(page)
    }

    fn validate_layout(&self, data_limit: usize) -> Result<()> {
        let data_offset = std::mem::offset_of!(Page, ptr);
        match self.page_type {
            Self::TYPE_META => ensure_end(self.id, data_offset, size_of::<Meta>(), data_limit),
            Self::TYPE_FREELIST => ensure_end(
                self.id,
                data_offset,
                checked_size(self.id, self.count, size_of::<RetiredPage>())?,
                data_limit,
            ),
            Self::TYPE_BRANCH => {
                let elements_size = checked_size(self.id, self.count, size_of::<BranchElement>())?;
                ensure_end(self.id, data_offset, elements_size, data_limit)?;
                for (index, element) in self.branch_elements().iter().enumerate() {
                    let element_offset = data_offset + index * size_of::<BranchElement>();
                    let relative_end =
                        element.pos.checked_add(element.key_size).ok_or_else(|| {
                            Error::InvalidDB(format!("page {} branch key overflow", self.id))
                        })?;
                    let relative_end = checked_usize(self.id, relative_end, "branch key end")?;
                    ensure_end(self.id, element_offset, relative_end, data_limit)?;
                }
                Ok(())
            }
            Self::TYPE_LEAF => {
                let elements_size = checked_size(self.id, self.count, size_of::<LeafElement>())?;
                ensure_end(self.id, data_offset, elements_size, data_limit)?;
                for (index, element) in self.leaf_elements().iter().enumerate() {
                    if !matches!(element.node_type, Node::TYPE_BUCKET | Node::TYPE_DATA) {
                        return Err(Error::InvalidDB(format!(
                            "page {} leaf {index} has invalid node type {}",
                            self.id, element.node_type
                        )));
                    }
                    let element_offset = data_offset + index * size_of::<LeafElement>();
                    let relative_end = element
                        .pos
                        .checked_add(element.key_size)
                        .and_then(|end| end.checked_add(element.value_size))
                        .ok_or_else(|| {
                            Error::InvalidDB(format!("page {} leaf data overflow", self.id))
                        })?;
                    let relative_end = checked_usize(self.id, relative_end, "leaf data end")?;
                    ensure_end(self.id, element_offset, relative_end, data_limit)?;
                }
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn meta(&self) -> &Meta {
        assert_eq!(
            self.page_type,
            Page::TYPE_META,
            "Did not find meta page, found {}",
            self.page_type
        );
        unsafe { &*(&self.ptr as *const u64 as *const Meta) }
    }

    pub(crate) fn meta_mut(&mut self) -> &mut Meta {
        assert_eq!(
            self.page_type,
            Page::TYPE_META,
            "Did not find meta page, found {}",
            self.page_type
        );
        unsafe { &mut *(&mut self.ptr as *mut u64 as *mut Meta) }
    }

    pub(crate) fn retired_pages(&self) -> &[RetiredPage] {
        assert_eq!(self.page_type, Page::TYPE_FREELIST);
        unsafe {
            let start = &self.ptr as *const u64 as *const RetiredPage;
            from_raw_parts(start, self.count as usize)
        }
    }

    pub(crate) fn retired_pages_mut(&mut self) -> &mut [RetiredPage] {
        assert_eq!(self.page_type, Page::TYPE_FREELIST);
        unsafe {
            let start = &self.ptr as *const u64 as *mut RetiredPage;
            from_raw_parts_mut(start, self.count as usize)
        }
    }

    pub(crate) fn leaf_elements(&self) -> &[LeafElement] {
        assert_eq!(
            self.page_type,
            Page::TYPE_LEAF,
            "Did not find leaf page, found {}",
            self.page_type
        );
        unsafe {
            let start = &self.ptr as *const u64 as *const LeafElement;
            from_raw_parts(start, self.count as usize)
        }
    }

    pub(crate) fn branch_elements(&self) -> &[BranchElement] {
        assert_eq!(
            self.page_type,
            Page::TYPE_BRANCH,
            "Did not find branch page, found {}",
            self.page_type
        );
        unsafe {
            let start = &self.ptr as *const u64 as *const BranchElement;
            from_raw_parts(start, self.count as usize)
        }
    }

    pub(crate) fn leaf_elements_mut(&mut self) -> &mut [LeafElement] {
        assert_eq!(
            self.page_type,
            Page::TYPE_LEAF,
            "Did not find leaf page, found {}",
            self.page_type
        );
        unsafe {
            let start = &self.ptr as *const u64 as *const LeafElement as *mut LeafElement;
            from_raw_parts_mut(start, self.count as usize)
        }
    }

    pub(crate) fn branch_elements_mut(&mut self) -> &mut [BranchElement] {
        assert_eq!(
            self.page_type,
            Page::TYPE_BRANCH,
            "Did not find branch page, found {}",
            self.page_type
        );
        unsafe {
            let start = &self.ptr as *const u64 as *const BranchElement as *mut BranchElement;
            from_raw_parts_mut(start, self.count as usize)
        }
    }

    fn slice(&mut self, size: u64) -> &mut [u8] {
        unsafe {
            let start = &self.ptr as *const u64 as *const u8 as *mut u8;
            from_raw_parts_mut(start, size as usize)
        }
    }

    pub(crate) fn write_node(&mut self, n: &Node, num_pages: u64) -> Result<()> {
        debug_assert!(self.id == n.page_id);
        debug_assert!(self.overflow == num_pages - 1);
        self.count = n.data.len() as u64;
        let header_size;
        let mut data_size: u64 = 0;
        let mut data: Vec<&[u8]>;
        match &n.data {
            NodeData::Branches(branches) => {
                self.page_type = Page::TYPE_BRANCH;
                header_size = size_of::<BranchElement>() as u64;
                let mut header_offsets = header_size * (branches.len() as u64);
                data = Vec::with_capacity(self.count as usize);
                let elems = self.branch_elements_mut();
                for (b, elem) in branches.iter().zip(elems.iter_mut()) {
                    debug_assert!(b.page > 1, "Branch should not point to page {}", b.page);
                    elem.page = b.page;
                    elem.key_size = b.key_size() as u64;
                    elem.pos = header_offsets + data_size;
                    data_size += elem.key_size;
                    header_offsets -= header_size;
                    data.push(b.key());
                }
            }
            NodeData::Leaves(leaves) => {
                self.page_type = Page::TYPE_LEAF;
                header_size = size_of::<LeafElement>() as u64;
                let mut header_offsets = header_size * (leaves.len() as u64);
                data = Vec::with_capacity(self.count as usize * 2);
                let elems = self.leaf_elements_mut();
                for (l, elem) in leaves.iter().zip(elems.iter_mut()) {
                    elem.node_type = l.node_type();

                    let key = l.key();
                    let value = l.value();
                    elem.key_size = key.len() as u64;
                    elem.value_size = value.len() as u64;
                    elem.pos = header_offsets + data_size;

                    data_size += elem.key_size + elem.value_size;
                    header_offsets -= header_size;

                    data.push(key);
                    data.push(value);
                }
            }
        };
        let total_header = header_size * self.count;
        let buf = self.slice(total_header + data_size);
        let mut buf = &mut buf[(total_header as usize)..];
        for b in data.iter() {
            buf.write_all(b)?;
        }
        Ok(())
    }
}

pub(crate) fn seal_block(buf: &mut [u8]) -> Result<()> {
    if buf.len() < CHECKSUM_SIZE {
        return Err(Error::InvalidDB(
            "cannot checksum a short page block".into(),
        ));
    }
    let checksum_at = buf.len() - CHECKSUM_SIZE;
    let hash = Sha3_256::digest(&buf[..checksum_at]);
    buf[checksum_at..].copy_from_slice(&hash);
    Ok(())
}

fn verify_checksum(buf: &[u8], id: PageID) -> Result<()> {
    let checksum_at = buf.len() - CHECKSUM_SIZE;
    let expected = Sha3_256::digest(&buf[..checksum_at]);
    if expected.as_slice() != &buf[checksum_at..] {
        return Err(Error::InvalidDB(format!("page {id} checksum mismatch")));
    }
    Ok(())
}

fn checked_size(id: PageID, count: u64, element_size: usize) -> Result<usize> {
    checked_usize(id, count, "element count")?
        .checked_mul(element_size)
        .ok_or_else(|| Error::InvalidDB(format!("page {id} element count overflow")))
}

fn checked_usize(id: PageID, value: u64, field: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| Error::InvalidDB(format!("page {id} {field} does not fit this platform")))
}

fn ensure_end(id: PageID, offset: usize, len: usize, limit: usize) -> Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| Error::InvalidDB(format!("page {id} data offset overflow")))?;
    if end > limit {
        return Err(Error::InvalidDB(format!(
            "page {id} data exceeds its block"
        )));
    }
    Ok(())
}

#[repr(C)]
pub(crate) struct BranchElement {
    pub(crate) page: PageID,
    key_size: u64,
    pos: u64,
}

impl BranchElement {
    pub(crate) fn key<'a>(&self) -> &'a [u8] {
        let pos = self.pos as usize;
        unsafe {
            let start = self as *const BranchElement as *const u8;
            let buf = std::slice::from_raw_parts(start, pos + (self.key_size as usize));
            &buf[pos..]
        }
    }
}

#[repr(C)]
pub(crate) struct LeafElement {
    pub(crate) node_type: NodeType,
    pos: u64,
    key_size: u64,
    value_size: u64,
}

impl LeafElement {
    pub(crate) fn key<'a>(&self) -> &'a [u8] {
        let pos = self.pos as usize;
        unsafe {
            let start = self as *const LeafElement as *const u8;
            let buf = std::slice::from_raw_parts(start, pos + self.key_size as usize);
            &buf[pos..]
        }
    }
    pub(crate) fn value<'a>(&self) -> &'a [u8] {
        let pos = (self.pos + self.key_size) as usize;
        unsafe {
            let start = self as *const LeafElement as *const u8;
            let buf = std::slice::from_raw_parts(start, pos + self.value_size as usize);
            &buf[pos..]
        }
    }
}
