use std::marker::PhantomData;
use std::sync::Arc;

/// Column family description.
///
/// # Example
///
/// ```rust
/// use weedb::{ColumnFamily,Caches};
/// use weedb::rocksdb::{Options, ReadOptions, WriteOptions, BlockBasedOptions, DataBlockIndexType};
///
/// struct Cells;
///
/// impl ColumnFamily for Cells {
///     const NAME: &'static str = "cells";
///
///     fn options(opts: &mut Options, caches: &Caches) {
///         opts.set_write_buffer_size(128 * 1024 * 1024);
///
///         let mut block_factory = BlockBasedOptions::default();
///         block_factory.set_block_cache(&caches.block_cache);
///         block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
///
///         opts.set_block_based_table_factory(&block_factory);
///
///         opts.set_optimize_filters_for_hits(true);
///     }
///
///     fn read_options(opts: &mut ReadOptions) {
///         opts.set_verify_checksums(false);
///     }
/// }
/// ```
pub trait ColumnFamily {
    const NAME: &'static str;

    /// Modify write options.
    fn write_options(opts: &mut rocksdb::WriteOptions) {
        let _unused = opts;
    }

    /// Modify read options.
    fn read_options(opts: &mut rocksdb::ReadOptions) {
        let _unused = opts;
    }
}

pub trait ColumnFamilyOptions<C>: ColumnFamily {
    /// Modify general options.
    fn options(opts: &mut rocksdb::Options, ctx: &mut C) {
        let _unused = opts;
        let _unused = ctx;
    }
}

/// RocksDB column family wrapper.
pub struct Table<T> {
    cf: CfHandle,
    db: Arc<rocksdb::DB>,
    write_config: rocksdb::WriteOptions,
    read_config: rocksdb::ReadOptions,
    _ty: PhantomData<T>,
}

impl<T> Table<T>
where
    T: ColumnFamily,
{
    /// Creates a column family wrapper instance.
    pub fn new(db: Arc<rocksdb::DB>) -> Self {
        use rocksdb::AsColumnFamilyRef;

        // Check that tree exists
        let cf = CfHandle(db.cf_handle(T::NAME).unwrap().inner());

        let mut write_config = Default::default();
        T::write_options(&mut write_config);

        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        Self {
            cf,
            db,
            write_config,
            read_config,
            _ty: Default::default(),
        }
    }

    /// Returns a bounded column family handle.
    pub fn cf(&'_ self) -> BoundedCfHandle<'_> {
        BoundedCfHandle {
            inner: self.cf.0,
            _lifetime: PhantomData,
        }
    }

    /// Returns an unbounded column family handle.
    pub fn get_unbounded_cf(&self) -> UnboundedCfHandle {
        UnboundedCfHandle {
            inner: self.cf.0,
            _db: self.db.clone(),
        }
    }

    /// Returns an inner rocksdb instance.
    #[inline]
    pub fn db(&self) -> &Arc<rocksdb::DB> {
        &self.db
    }

    /// Returns an existing read config.
    #[inline]
    pub fn read_config(&self) -> &rocksdb::ReadOptions {
        &self.read_config
    }

    /// Creates a new read config with options applied.
    pub fn new_read_config(&self) -> rocksdb::ReadOptions {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);
        read_config
    }

    /// Returns an existing write config.
    #[inline]
    pub fn write_config(&self) -> &rocksdb::WriteOptions {
        &self.write_config
    }

    /// Creates a new write config with options applied.
    pub fn new_write_config(&self) -> rocksdb::WriteOptions {
        let mut write_config = Default::default();
        T::write_options(&mut write_config);
        write_config
    }

    /// Gets a value from the DB.
    #[inline]
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        fn db_get<'a>(
            db: &'a rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            readopts: &rocksdb::ReadOptions,
        ) -> Result<Option<rocksdb::DBPinnableSlice<'a>>, rocksdb::Error> {
            db.get_pinned_cf_opt(&cf, key, readopts)
        }
        db_get(self.db.as_ref(), self.cf, key.as_ref(), &self.read_config)
    }

    /// Inserts a new value into the DB.
    #[inline]
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<(), rocksdb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        fn db_insert(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            value: &[u8],
            writeopts: &rocksdb::WriteOptions,
        ) -> Result<(), rocksdb::Error> {
            db.put_cf_opt(&cf, key, value, writeopts)
        }
        db_insert(
            self.db.as_ref(),
            self.cf,
            key.as_ref(),
            value.as_ref(),
            &self.write_config,
        )
    }

    /// Removes a value from the DB.
    #[allow(unused)]
    #[inline]
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<(), rocksdb::Error> {
        fn db_remove(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            writeopts: &rocksdb::WriteOptions,
        ) -> Result<(), rocksdb::Error> {
            db.delete_cf_opt(&cf, key, writeopts)
        }
        db_remove(self.db.as_ref(), self.cf, key.as_ref(), &self.write_config)
    }

    /// Checks whether the specified key is present in the DB.
    #[inline]
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, rocksdb::Error> {
        fn db_contains_key(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            readopts: &rocksdb::ReadOptions,
        ) -> Result<bool, rocksdb::Error> {
            match db.get_pinned_cf_opt(&cf, key, readopts) {
                Ok(value) => Ok(value.is_some()),
                Err(e) => Err(e),
            }
        }
        db_contains_key(self.db.as_ref(), self.cf, key.as_ref(), &self.read_config)
    }

    /// Creates an iterator with the specified mode and default read options.
    pub fn iterator(&'_ self, mode: rocksdb::IteratorMode) -> rocksdb::DBIterator<'_> {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.iterator_cf_opt(&self.cf, read_config, mode)
    }

    /// Creates a prefix iterator with the specified prefix and default read options.
    #[allow(unused)]
    pub fn prefix_iterator<P>(&'_ self, prefix: P) -> rocksdb::DBRawIterator<'_>
    where
        P: AsRef<[u8]>,
    {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);
        read_config.set_prefix_same_as_start(true);

        let mut iter = self.db.raw_iterator_cf_opt(&self.cf, read_config);
        iter.seek(prefix.as_ref());

        iter
    }

    /// Creates a raw iterator with default read options.
    pub fn raw_iterator(&'_ self) -> rocksdb::DBRawIterator<'_> {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.raw_iterator_cf_opt(&self.cf, read_config)
    }
}

/// Column family handle which is bounded to the DB instance.
#[derive(Copy, Clone)]
pub struct BoundedCfHandle<'a> {
    inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
    _lifetime: PhantomData<&'a ()>,
}

impl rocksdb::AsColumnFamilyRef for BoundedCfHandle<'_> {
    #[inline]
    fn inner(&self) -> *mut librocksdb_sys::rocksdb_column_family_handle_t {
        self.inner
    }
}

unsafe impl Send for BoundedCfHandle<'_> {}

/// Column family handle which could be moved into different thread.
#[derive(Clone)]
pub struct UnboundedCfHandle {
    inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
    _db: Arc<rocksdb::DB>,
}

impl UnboundedCfHandle {
    #[inline]
    pub fn bound(&self) -> BoundedCfHandle<'_> {
        BoundedCfHandle {
            inner: self.inner,
            _lifetime: PhantomData,
        }
    }
}

unsafe impl Send for UnboundedCfHandle {}
unsafe impl Sync for UnboundedCfHandle {}

#[derive(Copy, Clone)]
#[repr(transparent)]
struct CfHandle(*mut librocksdb_sys::rocksdb_column_family_handle_t);

impl rocksdb::AsColumnFamilyRef for CfHandle {
    #[inline]
    fn inner(&self) -> *mut librocksdb_sys::rocksdb_column_family_handle_t {
        self.0
    }
}

unsafe impl Send for CfHandle {}
unsafe impl Sync for CfHandle {}
