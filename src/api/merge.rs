use std::{rc::Rc, sync::Arc};

use crate::{
    Bucket, DB, Data, Error, Result, Tx,
    changes::{TTL_BUCKET, TTL_DEADLINES_BUCKET},
    ttl::decode_expiration,
};

#[cfg(feature = "changefeed")]
use crate::changes::JOURNAL_BUCKET;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Selects how a merge handles an existing destination entry.
pub enum MergeConflictPolicy {
    /// Replaces destination values and entry types with source data.
    #[default]
    Overwrite,
    /// Retains the destination entry and skips the conflicting source entry.
    KeepExisting,
    /// Returns [`Error::MergeConflict`] at the first conflicting path.
    Error,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Configures recursive database and bucket merges.
pub struct MergeOptions {
    conflict_policy: MergeConflictPolicy,
}

impl MergeOptions {
    /// Creates options using [`MergeConflictPolicy::Overwrite`].
    pub const fn new() -> Self {
        Self {
            conflict_policy: MergeConflictPolicy::Overwrite,
        }
    }

    /// Sets the conflict policy.
    pub const fn conflict_policy(mut self, policy: MergeConflictPolicy) -> Self {
        self.conflict_policy = policy;
        self
    }

    /// Returns the configured conflict policy.
    pub const fn policy(&self) -> MergeConflictPolicy {
        self.conflict_policy
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Counts changes performed by a merge.
pub struct MergeReport {
    /// Source keys absent from the destination.
    pub keys_inserted: u64,
    /// Destination records changed by source values or TTL metadata.
    pub keys_updated: u64,
    /// Matching keys whose values were already identical.
    pub keys_unchanged: u64,
    /// Conflicting entries retained by `KeepExisting`.
    pub entries_skipped: u64,
    /// Root or nested buckets created from the source.
    pub buckets_created: u64,
    /// Existing destination buckets recursively combined with source buckets.
    pub buckets_merged: u64,
    /// Key/value versus bucket conflicts replaced by `Overwrite`.
    pub type_conflicts_resolved: u64,
}

struct EntryMerge<'a> {
    options: MergeOptions,
    path: &'a [Vec<u8>],
    report: &'a mut MergeReport,
}

impl DB {
    /// Recursively merges every source root bucket into this database.
    ///
    /// The merge commits as one transaction. Destination entries not present in
    /// the source remain unchanged. TTL follows the winning key, and durable
    /// journal history is not imported from the source.
    pub fn merge_from(&self, source: &DB, options: MergeOptions) -> Result<MergeReport> {
        if Arc::ptr_eq(&self.inner, &source.inner) {
            return Ok(MergeReport::default());
        }
        let source_tx = source.read_tx()?;
        let destination_tx = self.write_tx()?;
        let report = merge_transactions(&destination_tx, &source_tx, options)?;
        destination_tx.commit()?;
        Ok(report)
    }
}

impl<'b, 'tx> Bucket<'b, 'tx> {
    /// Recursively merges a source bucket into this writable bucket.
    ///
    /// Changes belong to the destination bucket's existing transaction and are
    /// durable only after its caller commits that transaction.
    pub fn merge_from(
        &self,
        source: &Bucket<'_, '_>,
        options: MergeOptions,
    ) -> Result<MergeReport> {
        if !self.writable {
            return Err(Error::ReadOnlyTx);
        }
        if Rc::as_ptr(&self.inner).cast::<()>() == Rc::as_ptr(&source.inner).cast::<()>() {
            return Ok(MergeReport::default());
        }
        let mut report = MergeReport::default();
        merge_bucket(self, source, options, &mut Vec::new(), &mut report)?;
        Ok(report)
    }
}

fn merge_transactions(
    destination: &Tx<'_>,
    source: &Tx<'_>,
    options: MergeOptions,
) -> Result<MergeReport> {
    let mut report = MergeReport::default();
    for entry in source.buckets() {
        let (name, source_bucket) = entry?;
        #[cfg(feature = "changefeed")]
        if name.name() == JOURNAL_BUCKET {
            continue;
        }
        let name = name.name().to_vec();
        let destination_bucket = match destination.get_bucket(name.clone()) {
            Ok(bucket) => {
                report.buckets_merged += 1;
                bucket
            }
            Err(Error::BucketMissing) => {
                report.buckets_created += 1;
                destination.create_bucket(name.clone())?
            }
            Err(error) => return Err(error),
        };
        let mut path = vec![name];
        merge_bucket(
            &destination_bucket,
            &source_bucket,
            options,
            &mut path,
            &mut report,
        )?;
    }
    Ok(report)
}

fn merge_bucket(
    destination: &Bucket<'_, '_>,
    source: &Bucket<'_, '_>,
    options: MergeOptions,
    path: &mut Vec<Vec<u8>>,
    report: &mut MergeReport,
) -> Result<()> {
    let source_expirations = source.expiration_bucket()?;
    let manage_missing_ttl =
        source_expirations.is_some() || destination.expiration_bucket()?.is_some();
    for data in source.cursor() {
        let data = data?;
        match data {
            Data::KeyValue(pair) => merge_key_value(
                destination,
                source_expirations.as_ref(),
                manage_missing_ttl,
                pair.key(),
                pair.value(),
                EntryMerge {
                    options,
                    path,
                    report,
                },
            )?,
            Data::Bucket(name) => {
                if is_ttl_bucket(name.name()) {
                    continue;
                }
                let name = name.name().to_vec();
                let source_bucket = source.get_bucket(name.clone())?;
                merge_nested_bucket(destination, &source_bucket, name, options, path, report)?;
            }
        }
    }
    destination.merge_next_int(source.next_int());
    Ok(())
}

fn merge_key_value(
    destination: &Bucket<'_, '_>,
    source_expirations: Option<&Bucket<'_, '_>>,
    manage_missing_ttl: bool,
    key: &[u8],
    value: &[u8],
    merge: EntryMerge<'_>,
) -> Result<()> {
    let source_expiration = match source_expirations {
        Some(expirations) => expirations
            .get_kv(key)?
            .map(|expiration| decode_expiration(expiration.value()))
            .transpose()?,
        None => None,
    };
    let apply_ttl = match destination.get(key)? {
        None => {
            destination.put(key.to_vec(), value.to_vec())?;
            merge.report.keys_inserted += 1;
            true
        }
        Some(Data::KeyValue(existing)) if existing.value() == value => {
            match destination.expiration(key)? == source_expiration {
                true => {
                    merge.report.keys_unchanged += 1;
                    false
                }
                false => match merge.options.policy() {
                    MergeConflictPolicy::Overwrite => {
                        merge.report.keys_updated += 1;
                        true
                    }
                    MergeConflictPolicy::KeepExisting => {
                        merge.report.entries_skipped += 1;
                        false
                    }
                    MergeConflictPolicy::Error => return Err(merge_conflict(merge.path, key)),
                },
            }
        }
        Some(Data::KeyValue(_)) => match merge.options.policy() {
            MergeConflictPolicy::Overwrite => {
                destination.put(key.to_vec(), value.to_vec())?;
                merge.report.keys_updated += 1;
                true
            }
            MergeConflictPolicy::KeepExisting => {
                merge.report.entries_skipped += 1;
                false
            }
            MergeConflictPolicy::Error => return Err(merge_conflict(merge.path, key)),
        },
        Some(Data::Bucket(_)) => match merge.options.policy() {
            MergeConflictPolicy::Overwrite => {
                destination.delete_bucket(key.to_vec())?;
                destination.put(key.to_vec(), value.to_vec())?;
                merge.report.keys_inserted += 1;
                merge.report.type_conflicts_resolved += 1;
                true
            }
            MergeConflictPolicy::KeepExisting => {
                merge.report.entries_skipped += 1;
                false
            }
            MergeConflictPolicy::Error => return Err(merge_conflict(merge.path, key)),
        },
    };
    if apply_ttl {
        match source_expiration {
            Some(expires_at_millis) => {
                destination.set_expiration_millis(key, expires_at_millis)?;
            }
            None if manage_missing_ttl => {
                destination.clear_ttl(key)?;
            }
            None => {}
        }
    }
    Ok(())
}

fn is_ttl_bucket(name: &[u8]) -> bool {
    name == TTL_BUCKET || name == TTL_DEADLINES_BUCKET
}

fn merge_nested_bucket(
    destination: &Bucket<'_, '_>,
    source: &Bucket<'_, '_>,
    name: Vec<u8>,
    options: MergeOptions,
    path: &mut Vec<Vec<u8>>,
    report: &mut MergeReport,
) -> Result<()> {
    let destination_bucket = match destination.get(&name)? {
        None => {
            report.buckets_created += 1;
            destination.create_bucket(name.clone())?
        }
        Some(Data::Bucket(_)) => {
            report.buckets_merged += 1;
            destination.get_bucket(name.clone())?
        }
        Some(Data::KeyValue(_)) => match options.policy() {
            MergeConflictPolicy::Overwrite => {
                let _ = destination.delete(&name)?;
                report.buckets_created += 1;
                report.type_conflicts_resolved += 1;
                destination.create_bucket(name.clone())?
            }
            MergeConflictPolicy::KeepExisting => {
                report.entries_skipped += 1;
                return Ok(());
            }
            MergeConflictPolicy::Error => return Err(merge_conflict(path, &name)),
        },
    };
    path.push(name);
    let result = merge_bucket(&destination_bucket, source, options, path, report);
    path.pop();
    result
}

fn merge_conflict(path: &[Vec<u8>], key: &[u8]) -> Error {
    let mut path = path.to_vec();
    path.push(key.to_vec());
    Error::MergeConflict { path }
}
