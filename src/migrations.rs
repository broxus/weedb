use std::collections::HashMap;

use crate::{WeeDb, WeeDbRaw};

impl<T> WeeDb<T> {
    /// Applies the provided migrations set until the target version.
    pub fn apply<P>(&self, migrations: Migrations<P, Self>) -> Result<(), MigrationError>
    where
        P: VersionProvider,
    {
        apply_migrations(self, migrations)
    }
}

impl WeeDbRaw {
    /// Applies the provided migrations set until the target version.
    pub fn apply<P>(&self, migrations: Migrations<P, Self>) -> Result<(), MigrationError>
    where
        P: VersionProvider,
    {
        apply_migrations(self, migrations)
    }
}

fn apply_migrations<P: VersionProvider, D: AsRef<WeeDbRaw>>(
    db: &D,
    migrations: Migrations<P, D>,
) -> Result<(), MigrationError> {
    let raw = db.as_ref();
    if migrations.version_provider.get_version(raw)?.is_none() {
        tracing::info!("starting with empty db");

        return migrations
            .version_provider
            .set_version(raw, migrations.target_version);
    }

    loop {
        let version: Semver = migrations
            .version_provider
            .get_version(raw)?
            .ok_or(MigrationError::VersionNotFound)?;

        match version.cmp(&migrations.target_version) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                tracing::info!("stored DB version is compatible");
                break Ok(());
            }
            std::cmp::Ordering::Greater => {
                break Err(MigrationError::IncompatibleDbVersion {
                    version,
                    expected: migrations.target_version,
                })
            }
        }

        let migration = migrations
            .migrations
            .get(&version)
            .ok_or(MigrationError::MigrationNotFound(version))?;
        tracing::info!(?version, "applying migration");

        migrations
            .version_provider
            .set_version(raw, (*migration)(db)?)?;
    }
}

/// Migrations collection up to the target version.
pub struct Migrations<P, D = WeeDbRaw> {
    target_version: Semver,
    migrations: HashMap<Semver, Migration<D>>,
    version_provider: P,
}

impl<D> Migrations<DefaultVersionProvider, D> {
    /// Creates a migrations collection up to the specified version
    /// with the default version provider.
    pub fn with_target_version(target_version: Semver) -> Self {
        Self {
            target_version,
            migrations: Default::default(),
            version_provider: DefaultVersionProvider,
        }
    }
}

impl<P: VersionProvider, D> Migrations<P, D> {
    /// Creates a migrations collection up to the specified version
    /// with the specified version provider.
    pub fn with_target_version_and_provider(target_version: Semver, version_provider: P) -> Self {
        Self {
            target_version,
            migrations: Default::default(),
            version_provider,
        }
    }

    /// Registers a new migration.
    pub fn register<F>(
        &mut self,
        from: Semver,
        to: Semver,
        migration: F,
    ) -> Result<(), MigrationError>
    where
        F: Fn(&D) -> Result<(), MigrationError> + 'static,
    {
        use std::collections::hash_map;

        match self.migrations.entry(from) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(move |db| {
                    migration(db)?;
                    Ok(to)
                }));
                Ok(())
            }
            hash_map::Entry::Occupied(entry) => {
                Err(MigrationError::DuplicateMigration(*entry.key()))
            }
        }
    }
}

/// The simplest stored semver.
pub type Semver = [u8; 3];

type Migration<D = WeeDbRaw> = Box<dyn Fn(&D) -> Result<Semver, MigrationError>>;

pub trait VersionProvider {
    fn get_version(&self, db: &WeeDbRaw) -> Result<Option<Semver>, MigrationError>;
    fn set_version(&self, db: &WeeDbRaw, version: Semver) -> Result<(), MigrationError>;
}

/// A simple version provider.
///
/// Uses `weedb_version` entry in the `default` column family.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultVersionProvider;

impl DefaultVersionProvider {
    const DB_VERSION_KEY: &'static str = "weedb_version";
}

impl VersionProvider for DefaultVersionProvider {
    fn get_version(&self, db: &WeeDbRaw) -> Result<Option<Semver>, MigrationError> {
        match db.rocksdb().get(Self::DB_VERSION_KEY)? {
            Some(version) => version
                .try_into()
                .map_err(|_| MigrationError::InvalidDbVersion)
                .map(Some),
            None => Ok(None),
        }
    }

    fn set_version(&self, db: &WeeDbRaw, version: Semver) -> Result<(), MigrationError> {
        db.rocksdb()
            .put(Self::DB_VERSION_KEY, version)
            .map_err(MigrationError::DbError)
    }
}

/// Error type for migration related errors.
#[derive(thiserror::Error, Debug)]
pub enum MigrationError {
    #[error("incompatible DB version: {version:?}, expected {expected:?}")]
    IncompatibleDbVersion { version: Semver, expected: Semver },
    #[error("existing DB version not found")]
    VersionNotFound,
    #[error("invalid version")]
    InvalidDbVersion,
    #[error("migration not found: {0:?}")]
    MigrationNotFound(Semver),
    #[error("duplicate migration: {0:?}")]
    DuplicateMigration(Semver),
    #[error("db error: {0}")]
    DbError(#[from] rocksdb::Error),
    #[error("{0}")]
    Custom(#[source] Box<dyn std::error::Error>),
}
