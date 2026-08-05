use crate::{bucket::BucketMeta, page::PageID};

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[repr(C)]
#[derive(Debug, Clone)]
pub(crate) struct Meta {
    pub(crate) meta_page: u32,
    pub(crate) magic: u32,
    pub(crate) version: u32,
    pub(crate) pagesize: u64,
    pub(crate) root: BucketMeta,
    pub(crate) num_pages: PageID,
    pub(crate) freelist_page: PageID,
    pub(crate) tx_id: u64,
    pub(crate) hash: u64,
}

impl Meta {
    pub(crate) fn valid(&self) -> bool {
        self.hash == self.hash_self()
    }

    pub(crate) fn hash_self(&self) -> u64 {
        let mut hash = FNV_OFFSET_BASIS;
        hash = fnv1a(hash, &self.meta_page.to_be_bytes());
        hash = fnv1a(hash, &self.magic.to_be_bytes());
        hash = fnv1a(hash, &self.version.to_be_bytes());
        hash = fnv1a(hash, &self.pagesize.to_be_bytes());
        hash = fnv1a(hash, &self.root.root_page.to_be_bytes());
        hash = fnv1a(hash, &self.root.next_int.to_be_bytes());
        hash = fnv1a(hash, &self.num_pages.to_be_bytes());
        hash = fnv1a(hash, &self.freelist_page.to_be_bytes());
        fnv1a(hash, &self.tx_id.to_be_bytes())
    }
}

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta() {
        let mut meta = Meta {
            meta_page: 1,
            magic: 1_234_567_890,
            version: 987_654_321,
            pagesize: 4096,
            root: BucketMeta {
                root_page: 2,
                next_int: 2020,
            },
            num_pages: 13,
            freelist_page: 3,
            tx_id: 8,
            hash: 64,
        };

        assert!(!meta.valid());
        meta.hash = meta.hash_self();
        assert_eq!(meta.hash, meta.hash_self());

        meta.tx_id = 88;
        assert_ne!(meta.hash, meta.hash_self());

        meta.hash = meta.hash_self();
        assert_eq!(meta.hash, meta.hash_self());
    }
}
