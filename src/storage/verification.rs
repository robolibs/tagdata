use crate::{DB, Error, Result};

/// One failed storage invariant with the strongest location information known.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifyIssue {
    pub page_id: Option<u64>,
    pub offset: Option<u64>,
    pub page_kind: Option<String>,
    pub invariant: String,
    pub bucket_path: Vec<Vec<u8>>,
}

/// Structured result of a complete database verification attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifyReport {
    pub valid: bool,
    pub issues: Vec<VerifyIssue>,
}

impl VerifyReport {
    pub(crate) fn into_result(self) -> Result<()> {
        if self.valid {
            Ok(())
        } else {
            Err(Error::InvalidDB(
                self.issues
                    .first()
                    .map(|issue| issue.invariant.clone())
                    .unwrap_or_else(|| "verification failed".into()),
            ))
        }
    }
}

impl DB {
    /// Verifies the database and returns structured corruption information.
    pub fn verify_report(&self) -> Result<VerifyReport> {
        let tx = match self.tx(false) {
            Ok(tx) => tx,
            Err(Error::InvalidDB(invariant)) => {
                return Ok(report_issue(self, &invariant));
            }
            Err(error) => return Err(error),
        };
        match tx.check() {
            Ok(()) => Ok(VerifyReport {
                valid: true,
                issues: Vec::new(),
            }),
            Err(Error::InvalidDB(invariant)) => Ok(report_issue(self, &invariant)),
            Err(error) => Err(error),
        }
    }
}

fn report_issue(db: &DB, invariant: &str) -> VerifyReport {
    let page_id = extract_page_id(invariant);
    VerifyReport {
        valid: false,
        issues: vec![VerifyIssue {
            page_id,
            offset: page_id.and_then(|id| id.checked_mul(db.pagesize())),
            page_kind: infer_page_kind(invariant).or_else(|| page_kind(db, page_id?)),
            invariant: invariant.to_owned(),
            bucket_path: Vec::new(),
        }],
    }
}

fn page_kind(db: &DB, page_id: u64) -> Option<String> {
    let offset = page_id.checked_mul(db.pagesize())?.checked_add(8)? as usize;
    let data = db.inner.data.lock().ok()?;
    match *data.get(offset)? {
        1 => Some("branch".into()),
        2 => Some("leaf".into()),
        3 => Some("metadata".into()),
        4 => Some("freelist".into()),
        _ => None,
    }
}

fn extract_page_id(message: &str) -> Option<u64> {
    let mut words = message.split(|character: char| !character.is_ascii_alphanumeric());
    while let Some(word) = words.next() {
        if word.eq_ignore_ascii_case("page")
            && let Some(number) = words.find(|candidate| !candidate.is_empty())
            && let Ok(page) = number.parse()
        {
            return Some(page);
        }
    }
    None
}

fn infer_page_kind(message: &str) -> Option<String> {
    ["metadata", "freelist", "branch", "leaf", "overflow"]
        .into_iter()
        .find(|kind| message.to_ascii_lowercase().contains(kind))
        .map(str::to_owned)
}
