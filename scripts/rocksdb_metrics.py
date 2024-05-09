import sys
from typing import Callable

from grafanalib import formatunits as UNITS, _gen
from grafanalib.core import (
    Dashboard,
    Templating,
    Template,
    Annotations,
    RowPanel,
    TimeSeries,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    Target,
)

from common import (
    Layout,
    timeseries_panel,
    target,
    template,
    Expr,
    expr_sum_rate,
    expr_operator,
)


def convert_to_prometheus_metric(metric):
    return metric.replace(".", "_").replace("-", "_")


def create_graph_panel(
    metric_name, title, unit_format, labels=['cf=~"$cf"']
) -> TimeSeries:
    """Helper function to create a graph panel for a given metric."""
    return timeseries_panel(
        title=title,
        targets=[
            target(
                expr=Expr(
                    metric=convert_to_prometheus_metric(metric_name),
                    label_selectors=labels,
                ),
                legend_format="{{cf}} - {{instance}}",
            )
        ],
        unit=unit_format,
    )


def create_row(name, metrics) -> RowPanel:
    layout = Layout(name)
    for i in range(0, len(metrics), 2):
        chunk = metrics[i : i + 2]
        layout.row([create_graph_panel(*metric) for metric in chunk])
    return layout.row_panel


def mem_table_stats() -> RowPanel:
    memory_table_metrics = [
        (
            "rocksdb.num-immutable-mem-table",
            "Num Immutable Mem Table",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "rocksdb.cur-size-active-mem-table",
            "Current Size Active Mem Table",
            UNITS.BYTES_IEC,
        ),
        (
            "rocksdb.cur-size-all-mem-tables",
            "Current Size All Mem Tables",
            UNITS.BYTES_IEC,
        ),
        ("rocksdb.size-all-mem-tables", "Size All Mem Tables", UNITS.BYTES_IEC),
        (
            "rocksdb.num-entries-active-mem-table",
            "Num Entries Active Mem Table",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "rocksdb.num-entries-imm-mem-tables",
            "Num Entries Imm Mem Tables",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "rocksdb.num-deletes-active-mem-table",
            "Num Deletes Active Mem Table",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "rocksdb.num-deletes-imm-mem-tables",
            "Num Deletes Imm Mem Tables",
            UNITS.NUMBER_FORMAT,
        ),
    ]
    return create_row("Memtable stats", memory_table_metrics)


def sst_stats() -> RowPanel:
    metrics = [
        ("rocksdb.live-sst-files-size", "Live SST Files Size", UNITS.BYTES_IEC),
        ("rocksdb.num-files-at-level0", "Num Files at Level 0", UNITS.NUMBER_FORMAT),
        ("rocksdb.num-files-at-level1", "Num Files at Level 1", UNITS.NUMBER_FORMAT),
        ("rocksdb.num-files-at-level2", "Num Files at Level 2", UNITS.NUMBER_FORMAT),
        ("rocksdb.num-files-at-level3", "Num Files at Level 3", UNITS.NUMBER_FORMAT),
        ("rocksdb.num-files-at-level4", "Num Files at Level 4", UNITS.NUMBER_FORMAT),
        ("rocksdb.num-files-at-level5", "Num Files at Level 5", UNITS.NUMBER_FORMAT),
        ("rocksdb.num-files-at-level6", "Num Files at Level 6", UNITS.NUMBER_FORMAT),
    ]
    return create_row("Sst stats", metrics)


# ("rocksdb.num-running-flushes", "Count")
def flush_stats() -> RowPanel:
    layout = Layout("Flush stats")
    flush_metrics = [
        ("rocksdb.num-running-flushes", "Num Running Flushes", UNITS.NUMBER_FORMAT),
    ]
    layout.row([create_graph_panel(*metric) for metric in flush_metrics])
    return layout.row_panel


def compaction_stats() -> RowPanel:
    compaction_metrics = [
        ("rocksdb.compaction-pending", "Compaction Pending", UNITS.NUMBER_FORMAT),
        (
            "rocksdb.estimate-pending-compaction-bytes",
            "Estimate Pending Compaction Bytes",
            UNITS.BYTES_IEC,
        ),
        (
            "rocksdb.num-running-compactions",
            "Num Running Compactions",
            UNITS.NUMBER_FORMAT,
        ),
    ]
    return create_row("Compaction stats", compaction_metrics)


def misc_stats() -> RowPanel:
    misc_metrics = [
        ("rocksdb.background-errors", "Background Errors", UNITS.NUMBER_FORMAT),
        ("rocksdb.estimate-num-keys", "Estimate Num Keys", UNITS.NUMBER_FORMAT),
        (
            "rocksdb.estimate-table-readers-mem",
            "Estimate Table Readers Mem",
            UNITS.BYTES_IEC,
        ),
        ("rocksdb.num-live-versions", "Num Live Versions", UNITS.NUMBER_FORMAT),
        ("rocksdb.estimate-live-data-size", "Estimate Live Data Size", UNITS.BYTES_IEC),
        (
            "rocksdb.min-log-number-to-keep",
            "Min Log Number to Keep",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "rocksdb.actual-delayed-write-rate",
            "Actual Delayed Write Rate",
            UNITS.NUMBER_FORMAT,
        ),
    ]
    return create_row("Misc stats", misc_metrics)


# start of ROCKSDB_TICKERS


def block_cache_stats() -> RowPanel:
    layout = Layout("Block Cache stats")
    layout.row(
        [
            timeseries_panel(
                "Block Cache Bytes Read",
                unit=UNITS.BYTES_SEC_IEC,
                targets=[
                    target(expr=expr_sum_rate("rocksdb_block_cache_bytes_read")),
                    target(expr=expr_sum_rate("rocksdb_block_cache_bytes_write")),
                ],
            ),
        ]
    )

    # hits / (hits + misses)
    hit_rate = expr_operator(
        "rocksdb_block_cache_hit",
        "/",
        expr_operator("rocksdb_block_cache_hit", "+", "rocksdb_block_cache_miss"),
    )

    # useful / (useful + full_positive)
    bloom_hit_rate = expr_operator(
        "rocksdb_bloom_filter_useful",
        "/",
        expr_operator(
            "rocksdb_bloom_filter_useful", "+", "rocksdb_bloom_filter_full_positive"
        ),
    )

    layout.row(
        [
            timeseries_panel(
                "Block Cache Hit ratio",
                unit=UNITS.PERCENT_UNIT,
                targets=[target(expr=hit_rate)],
            ),
            timeseries_panel(
                "Bloom Filter stats",
                unit=UNITS.PERCENT_UNIT,
                targets=[target(expr=bloom_hit_rate)],
            ),
        ]
    )

    return layout.row_panel


def data_read_write_stats() -> RowPanel:
    layout = Layout("Data Read/Write stats")
    layout.row(
        [
            timeseries_panel(
                "Bytes Read/Written",
                unit=UNITS.BYTES_SEC_IEC,
                targets=[
                    target_with_legend("rocksdb_bytes_read", expr_sum_rate),
                    target_with_legend("rocksdb_bytes_written", expr_sum_rate),
                ],
            ),
            timeseries_panel(
                "Compact Read/Write Bytes",
                unit=UNITS.BYTES_SEC_IEC,
                targets=[
                    target_with_legend("rocksdb_compact_read_bytes", expr_sum_rate),
                    target_with_legend("rocksdb_compact_write_bytes", expr_sum_rate),
                ],
            ),
        ]
    )
    layout.row(
        [
            timeseries_panel(
                "Flush Write Bytes",
                unit=UNITS.BYTES_SEC_IEC,
                targets=[
                    target_with_legend("rocksdb_flush_write_bytes", expr_sum_rate)
                ],
            ),
            timeseries_panel(
                "Iterator Bytes Read",
                unit=UNITS.BYTES_SEC_IEC,
                targets=[
                    target_with_legend("rocksdb_db_iter_bytes_read", expr_sum_rate)
                ],
            ),
        ]
    )
    return layout.row_panel


def memtable_stats() -> RowPanel:
    layout = Layout("Memtable stats")
    layout.row(
        [
            timeseries_panel(
                "Memtable Hit/Miss",
                unit=UNITS.OPS_PER_SEC,
                targets=[
                    target_with_legend("rocksdb_memtable_hit", expr_sum_rate),
                    target_with_legend("rocksdb_memtable_miss", expr_sum_rate),
                ],
            )
        ]
    )
    return layout.row_panel


def iteration_seeking_stats() -> RowPanel:
    layout = Layout("Iteration and Seeking stats")
    layout.row(
        [
            timeseries_panel(
                "Iterator Created/Deleted",
                unit=UNITS.OPS_PER_SEC,
                targets=[
                    target_with_legend("rocksdb_num_iterator_created", expr_sum_rate),
                    target_with_legend("rocksdb_num_iterator_deleted", expr_sum_rate),
                ],
            ),
            timeseries_panel(
                "DB Next/Seek",
                unit=UNITS.OPS_PER_SEC,
                targets=[
                    target_with_legend("rocksdb_number_db_next", expr_sum_rate),
                    target_with_legend("rocksdb_number_db_seek", expr_sum_rate),
                ],
            ),
        ]
    )
    layout.row(
        [
            timeseries_panel(
                "Keys Read/Updated/Written",
                unit=UNITS.OPS_PER_SEC,
                targets=[
                    target_with_legend("rocksdb_number_keys_read", expr_sum_rate),
                    target_with_legend("rocksdb_number_keys_updated", expr_sum_rate),
                    target_with_legend("rocksdb_number_keys_written", expr_sum_rate),
                ],
            ),
            timeseries_panel(
                "Iteration Skip/Reseek",
                unit=UNITS.OPS_PER_SEC,
                targets=[
                    target_with_legend("rocksdb_number_iter_skip", expr_sum_rate),
                    target_with_legend(
                        "rocksdb_number_reseeks_iteration", expr_sum_rate
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def performance_timing_stats() -> RowPanel:
    layout = Layout("Performance and Timing stats")
    layout.row(
        [
            timeseries_panel(
                "Merge Operation Time",
                unit=UNITS.NANO_SECONDS,
                targets=[
                    target_with_legend(
                        "rocksdb_merge_operation_time_nanos", expr_sum_rate
                    )
                ],
            ),
            timeseries_panel(
                "Stall Time",
                unit=UNITS.MICRO_SECONDS,
                targets=[target_with_legend("rocksdb_stall_micros", expr_sum_rate)],
            ),
            timeseries_panel(
                "Compaction Cpu Time",
                unit=UNITS.MICRO_SECONDS,
                targets=[
                    target_with_legend(
                        "rocksdb_compaction_total_time_cpu_micros", expr_sum_rate
                    )
                ],
            ),
            timeseries_panel(
                "Filter Operation Time",
                unit=UNITS.NANO_SECONDS,
                targets=[
                    target_with_legend(
                        "rocksdb_filter_operation_time_nanos", expr_sum_rate
                    )
                ],
            ),
        ]
    )
    return layout.row_panel


def wal_stats() -> RowPanel:
    layout = Layout("Write-Ahead Log (WAL) stats")
    layout.row(
        [
            timeseries_panel(
                "WAL Bytes",
                unit=UNITS.BYTES_SEC_IEC,
                targets=[target_with_legend("rocksdb_wal_bytes", expr_sum_rate)],
            ),
            timeseries_panel(
                "WAL Synced/Written",
                unit=UNITS.OPS_PER_SEC,
                targets=[
                    target_with_legend("rocksdb_wal_synced", expr_sum_rate),
                    target_with_legend("rocksdb_write_wal", expr_sum_rate),
                ],
            ),
        ]
    )
    return layout.row_panel


# end of ROCKSDB_TICKERS


def target_with_legend(metric: str, expr_fn: Callable[[str], Expr]) -> Target:
    return target(expr=expr_fn(metric), legend_format="{{instance}} - %s" % metric)


def templates() -> Templating:
    return Templating(
        list=[
            Template(
                name="source",
                query="prometheus",
                type="datasource",
            ),
            template(
                name="instance",
                query="label_values(rocksdb_num_immutable_mem_table, instance)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="cf",
                query="label_values(rocksdb_num_immutable_mem_table, cf)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
        ]
    )


dashboard = Dashboard(
    "RocksDB Metrics",
    templating=templates(),
    refresh="1m",
    panels=[
        mem_table_stats(),
        sst_stats(),
        flush_stats(),
        compaction_stats(),
        misc_stats(),
        # start of ROCKSDB_TICKERS
        block_cache_stats(),
        data_read_write_stats(),
        memtable_stats(),
        iteration_seeking_stats(),
        performance_timing_stats(),
        wal_stats(),
        # end of ROCKSDB_TICKERS
    ],
    annotations=Annotations(),
    uid="cdleji62a1b0gb",
    version=9,
    schemaVersion=14,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    timezone="browser",
).auto_panel_ids()

# open file as stream
if len(sys.argv) > 1:
    stream = open(sys.argv[1], "w")
else:
    stream = sys.stdout
# write dashboard to file
_gen.write_dashboard(dashboard, stream)
