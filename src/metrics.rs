use metrics::{Label, Unit};
use rocksdb::statistics::{Histogram, HistogramData, Ticker};

use crate::WeeDbInner;

const ROCKSDB_TICKERS: &[(Ticker, Unit)] = &[
    (Ticker::BlockCacheBytesRead, Unit::Bytes),
    (Ticker::BlockCacheBytesWrite, Unit::Bytes),
    (Ticker::BlockCacheHit, Unit::Count),
    (Ticker::BlockCacheMiss, Unit::Count),
    (Ticker::BloomFilterUseful, Unit::Count),
    (Ticker::BloomFilterFullPositive, Unit::Count),
    (Ticker::BytesRead, Unit::Bytes),
    (Ticker::BytesWritten, Unit::Bytes),
    (Ticker::CompactReadBytes, Unit::Bytes),
    (Ticker::CompactWriteBytes, Unit::Bytes),
    (Ticker::FlushWriteBytes, Unit::Bytes),
    (Ticker::IterBytesRead, Unit::Bytes),
    (Ticker::MemtableHit, Unit::Count),
    (Ticker::MemtableMiss, Unit::Count),
    (Ticker::MergeOperationTotalTime, Unit::Nanoseconds),
    (Ticker::FilterOperationTotalTime, Unit::Nanoseconds),
    (Ticker::CompactionCpuTotalTime, Unit::Microseconds),
    (Ticker::NoIteratorCreated, Unit::Count),
    (Ticker::NoIteratorDeleted, Unit::Count),
    (Ticker::NumberDbNext, Unit::Count),
    (Ticker::NumberDbSeek, Unit::Count),
    (Ticker::NumberIterSkip, Unit::Count),
    (Ticker::NumberKeysRead, Unit::Count),
    (Ticker::NumberKeysUpdated, Unit::Count),
    (Ticker::NumberKeysWritten, Unit::Count),
    (Ticker::NumberOfReseeksInIteration, Unit::Count),
    (Ticker::StallMicros, Unit::Microseconds),
    (Ticker::WalFileBytes, Unit::Bytes),
    (Ticker::WalFileSynced, Unit::Count),
    (Ticker::WriteWithWal, Unit::Count),
];

const ROCKSDB_HISTOGRAMS: &[(Histogram, Unit)] = &[
    (Histogram::DbGet, Unit::Microseconds),
    (Histogram::DbMultiget, Unit::Microseconds),
    (Histogram::DbWrite, Unit::Microseconds),
    (Histogram::DbSeek, Unit::Microseconds),
    (Histogram::FlushTime, Unit::Microseconds),
    (Histogram::ReadBlockGetMicros, Unit::Microseconds),
    (Histogram::SstReadMicros, Unit::Microseconds),
    (Histogram::ReadNumMergeOperands, Unit::Count),
    (Histogram::NumSstReadPerLevel, Unit::Count),
    (Histogram::WalFileSyncMicros, Unit::Microseconds),
    (Histogram::PollWaitMicros, Unit::Microseconds),
    (Histogram::CompactionTime, Unit::Microseconds),
    (Histogram::SstBatchSize, Unit::Bytes),
    (Histogram::BytesPerWrite, Unit::Bytes),
    (Histogram::BytesPerRead, Unit::Bytes),
    (Histogram::NumSubcompactionsScheduled, Unit::Count),
];

// Per database properties
const ROCKSDB_DB_PROPERTIES: &[(&str, Unit)] = &[
    ("rocksdb.block-cache-capacity", Unit::Bytes),
    ("rocksdb.block-cache-usage", Unit::Bytes),
    ("rocksdb.block-cache-pinned-usage", Unit::Bytes),
];

// Per column-family properties
const ROCKSDB_CF_PROPERTIES: &[(&str, Unit)] = &[
    ("rocksdb.num-immutable-mem-table", Unit::Count),
    ("rocksdb.mem-table-flush-pending", Unit::Count),
    ("rocksdb.compaction-pending", Unit::Count),
    ("rocksdb.background-errors", Unit::Count),
    ("rocksdb.cur-size-active-mem-table", Unit::Bytes),
    ("rocksdb.cur-size-all-mem-tables", Unit::Bytes),
    ("rocksdb.size-all-mem-tables", Unit::Bytes),
    ("rocksdb.num-entries-active-mem-table", Unit::Count),
    ("rocksdb.num-entries-imm-mem-tables", Unit::Count),
    ("rocksdb.num-deletes-active-mem-table", Unit::Count),
    ("rocksdb.num-deletes-imm-mem-tables", Unit::Count),
    ("rocksdb.estimate-num-keys", Unit::Count),
    ("rocksdb.estimate-table-readers-mem", Unit::Bytes),
    ("rocksdb.num-live-versions", Unit::Count),
    ("rocksdb.estimate-live-data-size", Unit::Bytes),
    ("rocksdb.min-log-number-to-keep", Unit::Count),
    ("rocksdb.live-sst-files-size", Unit::Bytes),
    ("rocksdb.estimate-pending-compaction-bytes", Unit::Bytes),
    ("rocksdb.num-running-flushes", Unit::Count),
    ("rocksdb.num-running-compactions", Unit::Count),
    ("rocksdb.actual-delayed-write-rate", Unit::Count),
    // most often number of levels is 6
    ("rocksdb.num-files-at-level0", Unit::Count),
    ("rocksdb.num-files-at-level1", Unit::Count),
    ("rocksdb.num-files-at-level2", Unit::Count),
    ("rocksdb.num-files-at-level3", Unit::Count),
    ("rocksdb.num-files-at-level4", Unit::Count),
    ("rocksdb.num-files-at-level5", Unit::Count),
    ("rocksdb.num-files-at-level6", Unit::Count),
];

impl WeeDbInner {
    pub(crate) fn register_metrics(&self) {
        // Describe metrics
        for (ticker, unit) in ROCKSDB_TICKERS {
            let sanitized_name = sanitize_metric_name(ticker.name());
            metrics::describe_gauge!(sanitized_name, *unit, "");
        }

        // Register doesn't consume resource, so we don't hide it unlike exposing useless histograms
        for (histogram, unit) in ROCKSDB_HISTOGRAMS {
            let sanitized_name = sanitize_metric_name(histogram.name());
            for postfix in HISTOGRAM_POSTFIXES {
                let metric_name = format!("{sanitized_name}_{postfix}");
                metrics::describe_gauge!(metric_name, *unit, "");
            }
        }

        for (property, unit) in ROCKSDB_DB_PROPERTIES {
            let sanitized_name = sanitize_metric_name(property);
            metrics::describe_gauge!(sanitized_name, *unit, "");
        }
    }

    pub(crate) fn refresh_metrics(&self) {
        if !self.metrics_enabled {
            return;
        }

        let mut labels = Vec::with_capacity(2);
        if let Some(db_name) = self.db_name {
            labels.push(Label::from_static_parts("db", db_name));
        }

        let options = &self.options;
        let handles = self
            .cf_names
            .iter()
            .filter_map(|name| self.raw.cf_handle(name).map(|cf| (cf, name)));

        for (ticker, _) in ROCKSDB_TICKERS {
            let count = options.get_ticker_count(*ticker);

            let sanitized_name = sanitize_metric_name(ticker.name());
            metrics::gauge!(sanitized_name, labels.clone()).set(count as f64);
        }

        for (cf, name) in handles {
            for (property, _) in ROCKSDB_CF_PROPERTIES {
                let Ok(Some(value)) = self.raw.property_int_value_cf(&cf, *property) else {
                    continue;
                };

                let mut labels = labels.clone();
                labels.push(Label::from_static_parts("cf", name));

                let sanitized_name = sanitize_metric_name(property);
                metrics::gauge!(sanitized_name, labels).set(value as f64);
            }
        }
    }
}

const HISTOGRAM_POSTFIXES: &[&str] = &[
    "median",
    "percentile_95",
    "percentile_99",
    "average",
    "max",
    "standard_deviation",
];

#[allow(dead_code)]
// todo: use it after implementing histogram reset in rocksdb c-api
fn format_rocksdb_histogram_for_prometheus(name: &str, data: HistogramData, labels: &[Label]) {
    let metrics = [
        data.median(),
        data.p95(),
        data.p99(),
        data.average(),
        data.max(),
        data.std_dev(),
    ];

    let base_sanitized_name = sanitize_metric_name(name);

    for (value, suffix) in metrics.iter().zip(HISTOGRAM_POSTFIXES.iter()) {
        let metric_name = format!("{}_{}", base_sanitized_name, suffix);
        metrics::gauge!(metric_name, labels.iter()).set(*value);
    }
}

fn sanitize_metric_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
