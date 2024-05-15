use std::path::{Path, PathBuf};
use std::sync::Arc;

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

/// A thin wrapper around RocksDB.
#[derive(Clone)]
pub struct WeeDbRaw {
    inner: Arc<WeeDbInner>,
}

impl WeeDbRaw {
    /// Creates a DB builder.
    pub fn builder<P: AsRef<Path>, C: AsRef<Caches>>(path: P, context: C) -> RawBuilder<C> {
        RawBuilder::new(path, context)
    }

    /// Creates a table instance.
    pub fn instantiate_table<T: ColumnFamily>(&self) -> Table<T> {
        Table::new(self.inner.raw.clone())
    }

    /// Returns an underlying RocksDB instance.
    #[inline]
    pub fn raw(&self) -> &Arc<rocksdb::DB> {
        &self.inner.raw
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

impl AsRef<WeeDbRaw> for WeeDbRaw {
    #[inline]
    fn as_ref(&self) -> &WeeDbRaw {
        self
    }
}

struct WeeDbInner {
    raw: Arc<rocksdb::DB>,
    caches: Caches,
    db_name: Option<&'static str>,

    #[cfg(feature = "metrics")]
    options: rocksdb::Options,
    #[cfg(feature = "metrics")]
    cf_names: Vec<&'static str>,
    #[cfg(feature = "metrics")]
    metrics_enabled: bool,
}

impl WeeDbInner {
    fn get_memory_usage_stats(&self) -> Result<Stats, rocksdb::Error> {
        let whole_db_stats = rocksdb::perf::get_memory_usage_stats(
            Some(&[&self.raw]),
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

/// DB builder with a definition of all tables.
pub struct RawBuilder<C> {
    path: PathBuf,
    options: rocksdb::Options,
    context: C,
    descriptors: Vec<rocksdb::ColumnFamilyDescriptor>,
    cf_names: Vec<&'static str>,
    db_name: Option<&'static str>,
    #[cfg(feature = "metrics")]
    metrics_enabled: bool,
}

impl<C: AsRef<Caches>> RawBuilder<C> {
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
    pub fn options<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&mut rocksdb::Options, &mut C),
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

        let db = WeeDbInner {
            raw: Arc::new(rocksdb::DB::open_cf_descriptors(
                &self.options,
                self.path,
                self.descriptors,
            )?),
            caches: self.context.as_ref().clone(),
            db_name: self.db_name,
            #[cfg(feature = "metrics")]
            options: self.options,
            #[cfg(feature = "metrics")]
            cf_names: self.cf_names,
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
    fn caches_and_builder() -> Result<(), Box<dyn std::error::Error>> {
        let utime = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let db_path = format!("/tmp/weedb_{utime}");
        std::fs::create_dir_all(&db_path)?;

        // Prepare caches
        let caches = Caches::default();

        // Prepare db
        let db = WeeDbRaw::builder(&db_path, caches)
            .options(|opts, _| {
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
