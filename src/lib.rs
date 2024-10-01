#![doc = include_str!("../README.md")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub use rocksdb;

pub use self::caches::Caches;
pub use self::migrations::{
    DefaultVersionProvider, MigrationError, Migrations, Semver, VersionProvider,
};
pub use self::table::{
    BoundedCfHandle, ColumnFamily, ColumnFamilyOptions, Table, UnboundedCfHandle,
};

mod caches;
#[cfg(feature = "metrics")]
mod metrics;
mod migrations;
mod table;

/// Declares a group of tables as a struct.
///
/// # Example
///
/// ```rust
/// use weedb::{Caches, ColumnFamily, ColumnFamilyOptions};
///
/// struct MyTable;
///
/// impl ColumnFamily for MyTable {
///     const NAME: &'static str = "my_table";
/// }
///
/// impl ColumnFamilyOptions<Caches> for MyTable {}
///
/// weedb::tables! {
///     /// My tables.
///     pub struct MyTables<Caches> {
///         pub my_table: MyTable,
///     }
/// }
/// ```
#[macro_export]
macro_rules! tables {
    ($(#[$($meta:tt)*])* $pub:vis struct $ident:ident<$context:ty> {
        $($field_pub:vis $field:ident: $table:ty),+$(,)?
    }) => {
        $(#[$($meta)*])*
        $pub struct $ident {
            $($field_pub $field: $crate::Table<$table>,)*
        }

        impl $crate::Tables for $ident {
            type Context = $context;

            fn define(builder: $crate::WeeDbRawBuilder<Self::Context>) -> $crate::WeeDbRawBuilder<Self::Context> {
                builder$(.with_table::<$table>())*
            }

            fn instantiate(db: &$crate::WeeDbRaw) -> Self {
                Self {
                    $($field: db.instantiate_table(),)*
                }
            }

            fn column_families(&self) -> impl IntoIterator<Item = $crate::ColumnFamilyDescr<'_>> {
                [$($crate::ColumnFamilyDescr {
                    name: <$table as $crate::ColumnFamily>::NAME,
                    cf: self.$field.cf(),
                }),*]
            }
        }
    };
}

/// A group of tables that are used by the single database instance.
pub trait Tables: Send + Sync {
    /// Table creation context.
    type Context: AsRef<Caches>;

    /// Defines column families for the database.
    fn define(builder: WeeDbRawBuilder<Self::Context>) -> WeeDbRawBuilder<Self::Context>;

    /// Instantiates tables from the database.
    fn instantiate(db: &WeeDbRaw) -> Self;

    /// Returns a list of column families.
    fn column_families(&self) -> impl IntoIterator<Item = ColumnFamilyDescr<'_>>;
}

pub struct ColumnFamilyDescr<'a> {
    pub name: &'static str,
    pub cf: BoundedCfHandle<'a>,
}

/// [`WeeDb`] builder.
pub struct WeeDbBuilder<T: Tables> {
    inner: WeeDbRawBuilder<T::Context>,
}

impl<T: Tables> WeeDbBuilder<T> {
    /// Creates a DB builder.
    ///
    /// # Args
    /// - `path` - path to the DB directory. Will be created if not exists.
    /// - `context` - table creation context.
    pub fn new<P: AsRef<Path>>(path: P, context: T::Context) -> Self {
        Self {
            inner: WeeDbRawBuilder::new(path, context),
        }
    }

    /// Sets DB name. Used as label in metrics.
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.inner = self.inner.with_name(name);
        self
    }

    /// Modifies global options.
    pub fn with_options<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut rocksdb::Options, &mut T::Context),
    {
        self.inner = self.inner.with_options(f);
        self
    }

    /// Whether to enable RocksDB statistics.
    #[cfg(feature = "metrics")]
    pub fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.inner = self.inner.with_metrics_enabled(enabled);
        self
    }

    /// Opens a DB instance.
    #[allow(unused_mut)]
    pub fn build(mut self) -> Result<WeeDb<T>, rocksdb::Error> {
        let raw = self.inner.with_tables::<T>().build()?;

        Ok(WeeDb {
            inner: Arc::new(WeeDbInner {
                tables: T::instantiate(&raw),
                raw,
            }),
        })
    }
}

/// A thin wrapper around RocksDB and a group of tables.
#[repr(transparent)]
pub struct WeeDb<T> {
    inner: Arc<WeeDbInner<T>>,
}

impl<T: Tables> WeeDb<T> {
    /// Creates a DB builder.
    ///
    /// # Args
    /// - `path` - path to the DB directory. Will be created if not exists.
    /// - `context` - table creation context.
    pub fn builder<P: AsRef<Path>>(path: P, context: T::Context) -> WeeDbBuilder<T> {
        WeeDbBuilder::new(path, context)
    }

    pub async fn trigger_compaction(&self) {
        let mut compaction_options = rocksdb::CompactOptions::default();
        compaction_options.set_exclusive_manual_compaction(true);
        compaction_options
            .set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::ForceOptimized);

        for table in self.inner.tables.column_families() {
            tracing::info!(cf = table.name, "compaction started");

            let instant = Instant::now();
            let bound = Option::<[u8; 0]>::None;

            self.rocksdb()
                .compact_range_cf_opt(&table.cf, bound, bound, &compaction_options);

            tracing::info!(
                cf = table.name,
                elapsed_sec = %instant.elapsed().as_secs_f64(),
                "compaction finished"
            );
        }
    }
}

impl<T> WeeDb<T> {
    /// Returns a tables group.
    pub fn tables(&self) -> &T {
        &self.inner.tables
    }

    /// Returns an underlying RocksDB instance.
    pub fn rocksdb(&self) -> &Arc<rocksdb::DB> {
        self.inner.raw.rocksdb()
    }

    /// Creates a snapshot bounded to the DB instance.
    pub fn owned_snapshot(&self) -> OwnedSnapshot {
        OwnedSnapshot::new(self.rocksdb().clone())
    }

    /// Returns an underlying wrapper.
    pub fn raw(&self) -> &WeeDbRaw {
        &self.inner.raw
    }

    /// Returns a DB name if it was set.
    #[inline]
    pub fn db_name(&self) -> Option<&str> {
        self.inner.raw.db_name()
    }

    /// Returns an underlying caches group.
    #[inline]
    pub fn caches(&self) -> &Caches {
        self.inner.raw.caches()
    }

    /// Collects DB and cache memory usage stats.
    pub fn get_memory_usage_stats(&self) -> Result<Stats, rocksdb::Error> {
        self.inner.raw.get_memory_usage_stats()
    }

    /// Records RocksDB statistics into metrics.
    ///
    /// Does nothing if metrics are disabled.
    #[cfg(feature = "metrics")]
    pub fn refresh_metrics(&self) {
        self.inner.raw.refresh_metrics();
    }
}

impl<T> std::fmt::Debug for WeeDb<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let raw = self.inner.raw.as_ref();
        f.debug_struct("WeeDb")
            .field("db_name", &raw.inner.db_name)
            .field("tables", &raw.inner.cf_names)
            .finish()
    }
}

impl<T> Clone for WeeDb<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> AsRef<WeeDbRaw> for WeeDb<T> {
    #[inline]
    fn as_ref(&self) -> &WeeDbRaw {
        &self.inner.raw
    }
}

impl<T> std::ops::Deref for WeeDb<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner.tables
    }
}

struct WeeDbInner<T> {
    tables: T,
    raw: WeeDbRaw,
}

impl<T> Drop for WeeDbInner<T> {
    fn drop(&mut self) {
        self.raw.rocksdb().cancel_all_background_work(true);
    }
}

/// [`WeeDbRaw`] builder.
pub struct WeeDbRawBuilder<C> {
    path: PathBuf,
    options: rocksdb::Options,
    context: C,
    descriptors: Vec<rocksdb::ColumnFamilyDescriptor>,
    cf_names: Vec<&'static str>,
    db_name: Option<&'static str>,
    #[cfg(feature = "metrics")]
    metrics_enabled: bool,
}

impl<C: AsRef<Caches>> WeeDbRawBuilder<C> {
    /// Creates a DB builder.
    ///
    /// # Args
    /// - `path` - path to the DB directory. Will be created if not exists.
    /// - `context` - table creation context.
    pub fn new<P: AsRef<Path>>(path: P, context: C) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            options: Default::default(),
            context,
            descriptors: Default::default(),
            cf_names: Default::default(),
            db_name: None,
            #[cfg(feature = "metrics")]
            metrics_enabled: false,
        }
    }

    /// Sets DB name. Used as label in metrics.
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.db_name = Some(name);
        self
    }

    /// Modifies global options.
    pub fn with_options<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut rocksdb::Options, &mut C),
    {
        f(&mut self.options, &mut self.context);
        self
    }

    /// Adds a new column familty.
    pub fn with_table<T>(mut self) -> Self
    where
        T: ColumnFamilyOptions<C>,
    {
        let mut opts = Default::default();
        T::options(&mut opts, &mut self.context);
        self.descriptors
            .push(rocksdb::ColumnFamilyDescriptor::new(T::NAME, opts));
        self.cf_names.push(T::NAME);
        self
    }

    pub fn with_tables<T: Tables<Context = C>>(self) -> Self {
        T::define(self)
    }

    /// Whether to enable RocksDB statistics.
    #[cfg(feature = "metrics")]
    pub fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// Opens a DB instance.
    #[allow(unused_mut)]
    pub fn build(mut self) -> Result<WeeDbRaw, rocksdb::Error> {
        #[cfg(feature = "metrics")]
        if self.metrics_enabled {
            self.options.enable_statistics();
            self.options
                .set_statistics_level(rocksdb::statistics::StatsLevel::ExceptDetailedTimers);
        }

        let db = WeeDbRawInner {
            rocksdb: Arc::new(rocksdb::DB::open_cf_descriptors(
                &self.options,
                self.path,
                self.descriptors,
            )?),
            caches: self.context.as_ref().clone(),
            db_name: self.db_name,
            cf_names: self.cf_names,
            #[cfg(feature = "metrics")]
            options: self.options,
            #[cfg(feature = "metrics")]
            metrics_enabled: self.metrics_enabled,
        };

        #[cfg(feature = "metrics")]
        if self.metrics_enabled {
            db.register_metrics();
        }

        Ok(WeeDbRaw {
            inner: Arc::new(db),
        })
    }
}

/// A thin wrapper around RocksDB.
#[derive(Clone)]
#[repr(transparent)]
pub struct WeeDbRaw {
    inner: Arc<WeeDbRawInner>,
}

impl WeeDbRaw {
    /// Creates a DB builder.
    pub fn builder<P: AsRef<Path>, C: AsRef<Caches>>(path: P, context: C) -> WeeDbRawBuilder<C> {
        WeeDbRawBuilder::new(path, context)
    }

    /// Creates a table instance.
    pub fn instantiate_table<T: ColumnFamily>(&self) -> Table<T> {
        Table::new(self.inner.rocksdb.clone())
    }

    /// Returns an underlying RocksDB instance.
    #[inline]
    pub fn rocksdb(&self) -> &Arc<rocksdb::DB> {
        &self.inner.rocksdb
    }

    /// Creates a snapshot bounded to the DB instance.
    pub fn owned_snapshot(&self) -> OwnedSnapshot {
        OwnedSnapshot::new(self.rocksdb().clone())
    }

    /// Returns a DB name if it was set.
    #[inline]
    pub fn db_name(&self) -> Option<&'static str> {
        self.inner.db_name
    }

    /// Returns an underlying caches group.
    #[inline]
    pub fn caches(&self) -> &Caches {
        &self.inner.caches
    }

    /// Collects DB and cache memory usage stats.
    pub fn get_memory_usage_stats(&self) -> Result<Stats, rocksdb::Error> {
        self.inner.get_memory_usage_stats()
    }

    /// Records RocksDB statistics into metrics.
    ///
    /// Does nothing if metrics are disabled.
    #[cfg(feature = "metrics")]
    pub fn refresh_metrics(&self) {
        self.inner.refresh_metrics();
    }
}

impl std::fmt::Debug for WeeDbRaw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeeDbRaw")
            .field("db_name", &self.inner.db_name)
            .field("cf_names", &self.inner.cf_names)
            .finish()
    }
}

impl AsRef<WeeDbRaw> for WeeDbRaw {
    #[inline]
    fn as_ref(&self) -> &WeeDbRaw {
        self
    }
}

struct WeeDbRawInner {
    rocksdb: Arc<rocksdb::DB>,
    caches: Caches,
    db_name: Option<&'static str>,
    cf_names: Vec<&'static str>,

    #[cfg(feature = "metrics")]
    options: rocksdb::Options,
    #[cfg(feature = "metrics")]
    metrics_enabled: bool,
}

impl WeeDbRawInner {
    fn get_memory_usage_stats(&self) -> Result<Stats, rocksdb::Error> {
        let whole_db_stats = rocksdb::perf::get_memory_usage_stats(
            Some(&[&self.rocksdb]),
            Some(&[&self.caches.block_cache]),
        )?;

        let block_cache_usage = self.caches.block_cache.get_usage();
        let block_cache_pined_usage = self.caches.block_cache.get_pinned_usage();

        Ok(Stats {
            whole_db_stats,
            block_cache_usage,
            block_cache_pined_usage,
        })
    }
}

/// Memory usage stats.
pub struct Stats {
    pub whole_db_stats: rocksdb::perf::MemoryUsageStats,
    pub block_cache_usage: usize,
    pub block_cache_pined_usage: usize,
}

/// RocksDB snapshot bounded to a [`rocksdb::DB`] instance.
pub struct OwnedSnapshot {
    inner: rocksdb::Snapshot<'static>,
    _db: Arc<rocksdb::DB>,
}

impl OwnedSnapshot {
    pub fn new(db: Arc<rocksdb::DB>) -> Self {
        use rocksdb::Snapshot;

        unsafe fn extend_lifetime<'a>(r: Snapshot<'a>) -> Snapshot<'static> {
            std::mem::transmute::<Snapshot<'a>, Snapshot<'static>>(r)
        }

        // SAFETY: `Snapshot` requires the same lifetime as `rocksdb::DB` but
        // `tokio::task::spawn` requires 'static. This object ensures
        // that `rocksdb::DB` object lifetime will exceed the lifetime of the snapshot
        let inner = unsafe { extend_lifetime(db.as_ref().snapshot()) };
        Self { inner, _db: db }
    }
}

impl std::ops::Deref for OwnedSnapshot {
    type Target = rocksdb::Snapshot<'static>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// RocksDB raw iterator bounded to a [`rocksdb::DB`] instance.
pub struct OwnedRawIterator {
    inner: rocksdb::DBRawIterator<'static>,
    _db: Arc<rocksdb::DB>,
}

impl OwnedRawIterator {
    /// # Safety
    /// The following must be true:
    /// - `iter` must be created by the provided `db`.
    pub unsafe fn new(db: Arc<rocksdb::DB>, iter: rocksdb::DBRawIterator<'_>) -> Self {
        use rocksdb::DBRawIterator;

        unsafe fn extend_lifetime<'a>(r: DBRawIterator<'a>) -> DBRawIterator<'static> {
            std::mem::transmute::<DBRawIterator<'a>, DBRawIterator<'static>>(r)
        }

        // SAFETY: `DBRawIterator` requires the same lifetime as `rocksdb::DB` but
        // `tokio::task::spawn` requires 'static. This object ensures
        // that `rocksdb::DB` object lifetime will exceed the lifetime of the iterator
        let inner = unsafe { extend_lifetime(iter) };
        Self { inner, _db: db }
    }
}

impl std::ops::Deref for OwnedRawIterator {
    type Target = rocksdb::DBRawIterator<'static>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// RocksDB pinnable slice bounded to a [`rocksdb::DB`] instance.
pub struct OwnedPinnableSlice {
    inner: rocksdb::DBPinnableSlice<'static>,
    _db: Arc<rocksdb::DB>,
}

impl OwnedPinnableSlice {
    /// # Safety
    /// The following must be true:
    /// - `data` must be created by the provided `db`.
    pub unsafe fn new(db: Arc<rocksdb::DB>, data: rocksdb::DBPinnableSlice<'_>) -> Self {
        use rocksdb::DBPinnableSlice;

        unsafe fn extend_lifetime<'a>(r: DBPinnableSlice<'a>) -> DBPinnableSlice<'static> {
            std::mem::transmute::<DBPinnableSlice<'a>, DBPinnableSlice<'static>>(r)
        }

        // SAFETY: `DBPinnableSlice` requires the same lifetime as `rocksdb::DB` but
        // `tokio::task::spawn` requires 'static. This object ensures
        // that `rocksdb::DB` object lifetime will exceed the lifetime of the data
        let inner = unsafe { extend_lifetime(data) };
        Self { inner, _db: db }
    }
}

impl AsRef<[u8]> for OwnedPinnableSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::ops::Deref for OwnedPinnableSlice {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Describe column family
    struct MyTable;

    impl ColumnFamily for MyTable {
        // Column family name
        const NAME: &'static str = "my_table";

        // Modify read options
        fn read_options(opts: &mut rocksdb::ReadOptions) {
            opts.set_verify_checksums(false);
        }

        // Modify write options
        fn write_options(_: &mut rocksdb::WriteOptions) {
            // ...
        }
    }

    impl ColumnFamilyOptions<Caches> for MyTable {
        // Modify general options
        fn options(opts: &mut rocksdb::Options, caches: &mut Caches) {
            opts.set_write_buffer_size(128 * 1024 * 1024);

            let mut block_factory = rocksdb::BlockBasedOptions::default();
            block_factory.set_block_cache(&caches.block_cache);
            block_factory.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);

            opts.set_block_based_table_factory(&block_factory);

            opts.set_optimize_filters_for_hits(true);
        }
    }

    #[test]
    fn db_wrapper() -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempfile::tempdir()?;

        tables! {
            struct MyTables<Caches> {
                pub my_table: MyTable,
            }
        }

        let db = WeeDb::<MyTables>::builder(&tempdir, Caches::default())
            .with_name("test")
            .with_options(|opts, _| {
                // Do something with options:
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
            })
            .build()?;

        let mut migrations = Migrations::with_target_version([0, 1, 0]);
        migrations.register([0, 0, 0], [0, 1, 0], |_| {
            // do some migration stuff
            Ok(())
        })?;

        db.apply(migrations)?;

        db.tables().my_table.insert(b"123", b"321")?;
        let value = db.tables().my_table.get(b"123")?;
        assert_eq!(value.as_deref(), Some(b"321".as_slice()));

        Ok(())
    }

    #[test]
    fn caches_and_builder() -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempfile::tempdir()?;

        // Prepare caches
        let caches = Caches::default();

        // Prepare db
        let db = WeeDbRaw::builder(&tempdir, caches)
            .with_name("test")
            .with_options(|opts, _| {
                // Example configuration:

                opts.set_level_compaction_dynamic_level_bytes(true);

                // Compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

                // Logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // Cfs
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
            })
            .with_table::<MyTable>() // register column families
            .build()?;

        // Prepare and apply migration
        let mut migrations = Migrations::with_target_version([0, 1, 0]);
        migrations.register([0, 0, 0], [0, 1, 0], |_| {
            // do some migration stuff
            Ok(())
        })?;

        db.apply(migrations)?;

        // Table usage example
        let my_table = db.instantiate_table::<MyTable>();
        my_table.insert(b"asd", b"123")?;
        assert!(my_table.get(b"asd")?.is_some());

        Ok(())
    }
}
