use crate::{Error, Result};

const NODE_MAGIC: &[u8; 8] = b"INSPNODE";
const META_MAGIC: &[u8; 8] = b"INSPMETA";
const NODE_HEADER: usize = 40;
const META_VERSION: u32 = 1;

const LEAF: u8 = 1;
const BRANCH: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Meta {
    pub page_size: u32,
    pub txid: u64,
    pub root: u64,
    pub high_water: u64,
    pub freelist: u64,
}

#[derive(Debug)]
pub(crate) struct BuiltTree {
    pub root: u64,
    pub high_water: u64,
    pub pages: Vec<(u64, Vec<u8>)>,
}

#[derive(Clone, Debug)]
struct LevelNode {
    first_key: Vec<u8>,
    page: u64,
}

pub(crate) struct Node<'a> {
    bytes: &'a [u8],
    kind: u8,
    count: usize,
    payload_len: usize,
    span: usize,
    page: u64,
}

impl Meta {
    pub fn encode(self) -> Result<Vec<u8>> {
        let page_size = self.page_size as usize;
        if page_size < 1024 {
            return Err(Error::Corrupt("page size is too small"));
        }
        let mut page = vec![0; page_size];
        page[..8].copy_from_slice(META_MAGIC);
        page[8..12].copy_from_slice(&META_VERSION.to_le_bytes());
        page[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        page[16..24].copy_from_slice(&self.txid.to_le_bytes());
        page[24..32].copy_from_slice(&self.root.to_le_bytes());
        page[32..40].copy_from_slice(&self.high_water.to_le_bytes());
        page[40..48].copy_from_slice(&self.freelist.to_le_bytes());
        let sum = checksum(&page[..48]);
        page[48..56].copy_from_slice(&sum.to_le_bytes());
        Ok(page)
    }

    pub fn decode(page: &[u8]) -> Result<Self> {
        if page.len() < 56 || &page[..8] != META_MAGIC {
            return Err(Error::Corrupt("invalid meta page"));
        }
        if read_u32(page, 8)? != META_VERSION {
            return Err(Error::Corrupt("unsupported page format"));
        }
        if checksum(&page[..48]) != read_u64(page, 48)? {
            return Err(Error::Corrupt("meta checksum mismatch"));
        }
        let meta = Self {
            page_size: read_u32(page, 12)?,
            txid: read_u64(page, 16)?,
            root: read_u64(page, 24)?,
            high_water: read_u64(page, 32)?,
            freelist: read_u64(page, 40)?,
        };
        if meta.page_size < 1024 || !meta.page_size.is_power_of_two() {
            return Err(Error::Corrupt("invalid page size"));
        }
        Ok(meta)
    }
}

impl<'a> Node<'a> {
    pub fn decode(bytes: &'a [u8], page_size: usize) -> Result<Self> {
        if bytes.len() < NODE_HEADER || &bytes[..8] != NODE_MAGIC {
            return Err(Error::Corrupt("invalid node page"));
        }
        let kind = bytes[8];
        if !matches!(kind, LEAF | BRANCH) {
            return Err(Error::Corrupt("invalid node kind"));
        }
        let count = read_u32(bytes, 12)? as usize;
        let span = read_u32(bytes, 16)? as usize;
        let payload_len = read_u32(bytes, 20)? as usize;
        let page = read_u64(bytes, 32)?;
        let total = span
            .checked_mul(page_size)
            .ok_or(Error::Corrupt("node span overflow"))?;
        if span == 0 || total > bytes.len() || NODE_HEADER + payload_len > total {
            return Err(Error::Corrupt("invalid node span"));
        }
        if checksum(&bytes[NODE_HEADER..NODE_HEADER + payload_len]) != read_u64(bytes, 24)? {
            return Err(Error::Corrupt("node checksum mismatch"));
        }
        Ok(Self {
            bytes: &bytes[..total],
            kind,
            count,
            payload_len,
            span,
            page,
        })
    }

    pub fn page(&self) -> u64 {
        self.page
    }

    pub fn span(&self) -> usize {
        self.span
    }

    pub fn leaf_records(&self) -> Result<Vec<(&'a [u8], &'a [u8])>> {
        if self.kind != LEAF {
            return Err(Error::Corrupt("expected leaf node"));
        }
        let mut cursor = NODE_HEADER;
        let end = NODE_HEADER + self.payload_len;
        let mut records = Vec::with_capacity(self.count);
        for _ in 0..self.count {
            let key_len = read_u32(self.bytes, cursor)? as usize;
            let value_len = usize::try_from(read_u64(self.bytes, cursor + 4)?)
                .map_err(|_| Error::Corrupt("value length overflow"))?;
            cursor += 12;
            let key_end = cursor
                .checked_add(key_len)
                .ok_or(Error::Corrupt("leaf length overflow"))?;
            let value_end = key_end
                .checked_add(value_len)
                .ok_or(Error::Corrupt("leaf length overflow"))?;
            if value_end > end {
                return Err(Error::Corrupt("leaf record exceeds node"));
            }
            records.push((
                &self.bytes[cursor..key_end],
                &self.bytes[key_end..value_end],
            ));
            cursor = value_end;
        }
        if cursor != end {
            return Err(Error::Corrupt("leaf has trailing bytes"));
        }
        Ok(records)
    }

    pub fn branches(&self) -> Result<Vec<(&'a [u8], u64)>> {
        if self.kind != BRANCH {
            return Err(Error::Corrupt("expected branch node"));
        }
        let mut cursor = NODE_HEADER;
        let end = NODE_HEADER + self.payload_len;
        let mut branches = Vec::with_capacity(self.count);
        for _ in 0..self.count {
            let key_len = read_u32(self.bytes, cursor)? as usize;
            let child = read_u64(self.bytes, cursor + 4)?;
            cursor += 12;
            let key_end = cursor
                .checked_add(key_len)
                .ok_or(Error::Corrupt("branch length overflow"))?;
            if key_end > end {
                return Err(Error::Corrupt("branch record exceeds node"));
            }
            branches.push((&self.bytes[cursor..key_end], child));
            cursor = key_end;
        }
        if cursor != end {
            return Err(Error::Corrupt("branch has trailing bytes"));
        }
        Ok(branches)
    }
}

pub(crate) fn build_tree(records: &[Record], page_size: usize, start: u64) -> Result<BuiltTree> {
    if page_size < 1024 || !page_size.is_power_of_two() {
        return Err(Error::Corrupt("invalid page size"));
    }
    let mut sorted = records.to_vec();
    sorted.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    if sorted.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(Error::Corrupt("duplicate tree key"));
    }

    let mut pages = Vec::new();
    let mut next_page = start;
    let mut level = Vec::new();
    let mut batch = Vec::new();
    let mut size = NODE_HEADER;
    for record in sorted {
        let record_size = 12 + record.key.len() + record.value.len();
        if !batch.is_empty() && size + record_size > page_size {
            let node = encode_leaf(&batch, page_size, next_page)?;
            let span = node.len() / page_size;
            level.push(LevelNode {
                first_key: batch[0].key.clone(),
                page: next_page,
            });
            pages.push((next_page, node));
            next_page += span as u64;
            batch.clear();
            size = NODE_HEADER;
        }
        size += record_size;
        batch.push(record);
    }
    if batch.is_empty() {
        batch.push(Record {
            key: Vec::new(),
            value: Vec::new(),
        });
    }
    let node = encode_leaf(&batch, page_size, next_page)?;
    let span = node.len() / page_size;
    level.push(LevelNode {
        first_key: batch[0].key.clone(),
        page: next_page,
    });
    pages.push((next_page, node));
    next_page += span as u64;

    while level.len() > 1 {
        let mut parent = Vec::new();
        let mut cursor = 0;
        while cursor < level.len() {
            let mut end = cursor;
            let mut used = NODE_HEADER;
            while end < level.len() {
                let entry_size = 12 + level[end].first_key.len();
                if end > cursor && used + entry_size > page_size {
                    break;
                }
                used += entry_size;
                end += 1;
            }
            let node = encode_branch(&level[cursor..end], page_size, next_page)?;
            let span = node.len() / page_size;
            parent.push(LevelNode {
                first_key: level[cursor].first_key.clone(),
                page: next_page,
            });
            pages.push((next_page, node));
            next_page += span as u64;
            cursor = end;
        }
        level = parent;
    }

    Ok(BuiltTree {
        root: level[0].page,
        high_water: next_page,
        pages,
    })
}

fn encode_leaf(records: &[Record], page_size: usize, page: u64) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    for record in records {
        let key_len = u32::try_from(record.key.len()).map_err(|_| Error::TooLarge)?;
        let value_len = u64::try_from(record.value.len()).map_err(|_| Error::TooLarge)?;
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&value_len.to_le_bytes());
        payload.extend_from_slice(&record.key);
        payload.extend_from_slice(&record.value);
    }
    encode_node(LEAF, records.len(), &payload, page_size, page)
}

fn encode_branch(nodes: &[LevelNode], page_size: usize, page: u64) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    for node in nodes {
        let key_len = u32::try_from(node.first_key.len()).map_err(|_| Error::TooLarge)?;
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&node.page.to_le_bytes());
        payload.extend_from_slice(&node.first_key);
    }
    encode_node(BRANCH, nodes.len(), &payload, page_size, page)
}

fn encode_node(
    kind: u8,
    count: usize,
    payload: &[u8],
    page_size: usize,
    page: u64,
) -> Result<Vec<u8>> {
    let count = u32::try_from(count).map_err(|_| Error::TooLarge)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| Error::TooLarge)?;
    let total = NODE_HEADER
        .checked_add(payload.len())
        .ok_or(Error::TooLarge)?;
    let span = total.div_ceil(page_size);
    let span_u32 = u32::try_from(span).map_err(|_| Error::TooLarge)?;
    let mut bytes = vec![0; span * page_size];
    bytes[..8].copy_from_slice(NODE_MAGIC);
    bytes[8] = kind;
    bytes[12..16].copy_from_slice(&count.to_le_bytes());
    bytes[16..20].copy_from_slice(&span_u32.to_le_bytes());
    bytes[20..24].copy_from_slice(&payload_len.to_le_bytes());
    bytes[24..32].copy_from_slice(&checksum(payload).to_le_bytes());
    bytes[32..40].copy_from_slice(&page.to_le_bytes());
    bytes[NODE_HEADER..NODE_HEADER + payload.len()].copy_from_slice(payload);
    Ok(bytes)
}

fn checksum(bytes: &[u8]) -> u64 {
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
        .ok_or(Error::Corrupt("unexpected end of page"))?;
    Ok(u32::from_le_bytes(raw.try_into().expect("length checked")))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let raw = bytes
        .get(offset..offset + 8)
        .ok_or(Error::Corrupt("unexpected end of page"))?;
    Ok(u64::from_le_bytes(raw.try_into().expect("length checked")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_round_trip_and_checksum() {
        let meta = Meta {
            page_size: 4096,
            txid: 7,
            root: 42,
            high_water: 99,
            freelist: 12,
        };
        let mut encoded = meta.encode().unwrap();
        assert_eq!(Meta::decode(&encoded).unwrap(), meta);
        encoded[24] ^= 1;
        assert!(Meta::decode(&encoded).is_err());
    }

    #[test]
    fn builds_multi_level_tree_with_overflow_values() {
        let records = (0_u32..500)
            .map(|number| Record {
                key: number.to_be_bytes().to_vec(),
                value: if number == 250 {
                    vec![9; 12_000]
                } else {
                    number.to_le_bytes().repeat(8)
                },
            })
            .collect::<Vec<_>>();
        let tree = build_tree(&records, 1024, 2).unwrap();
        assert!(tree.pages.len() > 2);
        assert!(tree.high_water > tree.root);
        for (page, bytes) in &tree.pages {
            let node = Node::decode(bytes, 1024).unwrap();
            assert_eq!(node.page(), *page);
            if node.kind == LEAF {
                node.leaf_records().unwrap();
            } else {
                node.branches().unwrap();
            }
        }
    }
}
