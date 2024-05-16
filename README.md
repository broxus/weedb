## WeeDB &emsp; [![crates-io-batch]][crates-io-link] [![docs-badge]][docs-url] [![rust-version-badge]][rust-version-link]

[crates-io-batch]: https://img.shields.io/crates/v/weedb.svg

[crates-io-link]: https://crates.io/crates/weedb

[docs-badge]: https://docs.rs/weedb/badge.svg

[docs-url]: https://docs.rs/weedb

[rust-version-badge]: https://img.shields.io/badge/rustc-1.65+-lightgray.svg

[rust-version-link]: https://blog.rust-lang.org/2022/11/03/Rust-1.65.0.html


A thin wrapper around RocksDB.

### Example

```rust
use weedb::{rocksdb, Caches, ColumnFamily, ColumnFamilyOptions, Migrations, WeeDb};

// Describe tables group via macros.
//
// A group is parametrized with a table creation context.
weedb::tables! {
    pub struct MyTables<Caches> {
        pub my_table: MyTable,
        // ..and some other tables as fields...
    }
}

// Describe a column family.
struct MyTable;

impl ColumnFamily for MyTable {
    // Column family name
    const NAME: &'static str = "my_table";

    // Modify read options
    fn read_options(opts: &mut rocksdb::ReadOptions) {
        opts.set_verify_checksums(false);
    }

    // Modify write options
    fn write_options(opts: &mut rocksdb::WriteOptions) {
        // ...
    }
}

// Implement cf options setup for some specific context.
impl ColumnFamilyOptions<Caches> for MyTable {
    // Modify general options
    fn options(opts: &mut rocksdb::Options, caches: &mut Caches) {
        opts.set_write_buffer_size(128 * 1024 * 1024);

        // Use cache from the context
        let mut block_factory = rocksdb::BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);
        block_factory.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);

        opts.set_block_based_table_factory(&block_factory);

        opts.set_optimize_filters_for_hits(true);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = tempfile::tempdir()?;

    // Prepare caches
    let caches = Caches::with_capacity(10 << 23);

    // Prepare db
    let db = WeeDb::<MyTables>::builder(&tempdir, caches)
        .with_name("test")
        .with_metrics_enabled(true)
        .with_options(|opts, _ctx| {
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
        .build()?;

    // Prepare and apply migration
    let mut migrations = Migrations::with_target_version([0, 1, 0]);
    migrations.register([0, 0, 0], [0, 1, 0], |db| {
        // do some migration stuff
        Ok(())
    })?;

    db.apply(migrations)?;

    // Table usage example
    let my_table = &db.tables().my_table;
    my_table.insert(b"asd", b"123")?;
    assert!(my_table.get(b"asd")?.is_some());

    Ok(())
}
```

# How to generate grafana dashboard

```bash
cd scripts
python3 -m venv venv
# activate venv according to your shell (source venv/bin/activate)
pip install -r requirements.txt
python rocksdb_metrics.py dashboard.json
```

## Contributing

We welcome contributions to the project! If you notice any issues or errors,
feel free to open an issue or submit a pull request.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)
  or <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT)
  or <http://opensource.org/licenses/MIT>)

at your option.
