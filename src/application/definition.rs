use std::marker::PhantomData;

/// Controls how a typed collection is opened in a write transaction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OpenPolicy {
    /// The collection must already exist.
    Existing,
    /// The collection must not already exist.
    Create,
    /// Open the collection or create it when absent.
    #[default]
    CreateOrOpen,
}

/// A cheap, reusable description of a typed root collection.
///
/// Definitions contain no transaction state and are intended to be declared as
/// `const` or `static` values and shared across an application.
#[derive(Clone, Copy, Debug)]
pub struct CollectionDef<K, V, C> {
    pub(crate) name: &'static str,
    pub(crate) schema_id: &'static str,
    pub(crate) schema_version: u32,
    pub(crate) policy: OpenPolicy,
    pub(crate) codec: C,
    pub(crate) marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C> CollectionDef<K, V, C> {
    /// Defines a root collection. Its initial schema identity is its name and
    /// its initial schema version is one.
    pub const fn new(name: &'static str, codec: C) -> Self {
        Self {
            name,
            schema_id: name,
            schema_version: 1,
            policy: OpenPolicy::CreateOrOpen,
            codec,
            marker: PhantomData,
        }
    }

    /// Sets the stable codec/schema identity and version stored in the file.
    pub const fn schema(mut self, id: &'static str, version: u32) -> Self {
        self.schema_id = id;
        self.schema_version = version;
        self
    }

    pub const fn open_policy(mut self, policy: OpenPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn schema_id(&self) -> &'static str {
        self.schema_id
    }

    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub const fn policy(&self) -> OpenPolicy {
        self.policy
    }
}
