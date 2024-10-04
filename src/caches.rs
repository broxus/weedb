/// A group of caches used by the DB.
#[derive(Clone)]
pub struct Caches {
    pub block_cache: rocksdb::Cache,
}

impl AsRef<Caches> for Caches {
    #[inline]
    fn as_ref(&self) -> &Caches {
        self
    }
}

impl Default for Caches {
    fn default() -> Self {
        Self::with_capacity(Self::MIN_CAPACITY)
    }
}

impl Caches {
    const MIN_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB

    /// Creates a new instance with the specified capacity (in bytes).
    ///
    /// NOTE: if the specified capacity is too low it will be clamped to 64 MB.
    pub fn with_capacity(capacity: usize) -> Self {
        let block_cache_capacity = std::cmp::max(capacity, Self::MIN_CAPACITY);

        Self {
            block_cache: rocksdb::Cache::new_lru_cache(block_cache_capacity),
        }
    }

    /// Sets cache capacity in bytes.
    ///
    /// NOTE: if the specified capacity is too low it will be clamped to 64 MB.
    pub fn set_capacity(&self, capacity: usize) {
        let block_cache_capacity = std::cmp::max(capacity, Self::MIN_CAPACITY);

        self.block_cache.clone().set_capacity(block_cache_capacity);
    }
}
