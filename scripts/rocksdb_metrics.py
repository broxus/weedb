import sys
from grafanalib import formatunits as UNITS, _gen
from grafanalib.core import (
    Dashboard,
    Templating,
    Template,
    Annotations,
    Target,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
)
from dashboard_builder import (
    Layout,
    timeseries_panel,
    target,
    template,
    Expr,
    expr_sum_rate,
    expr_operator,
)


def convert_metric(metric):
    return metric.replace(".", "_").replace("-", "_")


def make_panel(expr, title, unit_format=UNITS.NUMBER_FORMAT, labels=None):
    if labels is None:
        labels = ['cf=~"$cf"']

    if isinstance(expr, str):
        expr = [Expr(metric=expr, label_selectors=labels)]
    elif isinstance(expr, list):
        expr = [
            Expr(metric=e, label_selectors=labels) if isinstance(e, str) else e
            for e in expr
        ]

    legend = "{{instance}}" + "".join(
        f" {l.split('=')[0]}:{{{{{l.split('=')[0]}}}}}" for l in labels
    )
    return timeseries_panel(
        title=title,
        targets=[target(e, legend_format=legend) for e in expr],
        unit=unit_format,
    )


def make_row(name, metrics, repeat=None, collapsed=True):
    layout = Layout(name, repeat=repeat, collapsed=collapsed)
    for i in range(0, len(metrics), 2):
        layout.row([make_panel(*m) for m in metrics[i : i + 2]])
    return layout.row_panel


def memtable_metrics():
    base_metrics = [
        ("num-immutable-mem-table", "Num Immutable Mem Table", UNITS.NUMBER_FORMAT),
        ("cur-size-active-mem-table", "Current Size Active Mem Table", UNITS.BYTES_IEC),
        ("cur-size-all-mem-tables", "Current Size All Mem Tables", UNITS.BYTES_IEC),
        ("size-all-mem-tables", "Size All Mem Tables", UNITS.BYTES_IEC),
        (
            "num-entries-active-mem-table",
            "Num Entries Active Mem Table",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "num-entries-imm-mem-tables",
            "Num Entries Imm Mem Tables",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "num-deletes-active-mem-table",
            "Num Deletes Active Mem Table",
            UNITS.NUMBER_FORMAT,
        ),
        (
            "num-deletes-imm-mem-tables",
            "Num Deletes Imm Mem Tables",
            UNITS.NUMBER_FORMAT,
        ),
    ]
    return make_row(
        "Memtable stats",
        [(convert_metric(f"rocksdb.{m}"), t, u) for m, t, u in base_metrics],
    )


def sst_metrics():
    metrics = [("live-sst-files-size", "Live SST Files Size", UNITS.BYTES_IEC)]
    metrics.extend(
        [
            (f"num-files-at-level{i}", f"Num Files at Level {i}", UNITS.NUMBER_FORMAT)
            for i in range(7)
        ]
    )
    return make_row(
        "SST stats", [(convert_metric(f"rocksdb.{m}"), t, u) for m, t, u in metrics]
    )


def blob_db_metrics():
    base_metrics = [
        # Blob cache metrics
        ("blob-cache-capacity", "Blob Cache Capacity", UNITS.BYTES_IEC),
        ("blob-cache-usage", "Blob Cache Usage", UNITS.BYTES_IEC),
        ("blob-cache-pinned-usage", "Blob Cache Pinned Usage", UNITS.BYTES_IEC),
        # Blob file metrics
        ("num-blob-files", "Number of Blob Files", UNITS.NUMBER_FORMAT),
        ("total-blob-file-size", "Total Blob File Size", UNITS.BYTES_IEC),
        ("live-blob-file-size", "Live Blob File Size", UNITS.BYTES_IEC),
        ("live-blob-file-garbage-size", "Live Blob File Garbage Size", UNITS.BYTES_IEC),
    ]
    return make_row(
        "BlobDB stats",
        [(convert_metric(f"rocksdb.{m}"), t, u) for m, t, u in base_metrics],
    )


def flush_metrics():
    return make_row(
        "Flush stats",
        [
            (
                convert_metric("rocksdb.num-running-flushes"),
                "Num Running Flushes",
                UNITS.NUMBER_FORMAT,
            )
        ],
    )


def compaction_metrics():
    base_metrics = [
        ("compaction-pending", "Compaction Pending", UNITS.NUMBER_FORMAT),
        (
            "estimate-pending-compaction-bytes",
            "Estimate Pending Compaction Bytes",
            UNITS.BYTES_IEC,
        ),
        ("num-running-compactions", "Num Running Compactions", UNITS.NUMBER_FORMAT),
    ]
    return make_row(
        "Compaction stats",
        [(convert_metric(f"rocksdb.{m}"), t, u) for m, t, u in base_metrics],
    )


def block_cache_metrics():
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
        Expr("rocksdb_block_cache_hit"),
        "/",
        expr_operator(
            Expr("rocksdb_block_cache_hit"), "+", Expr("rocksdb_block_cache_miss")
        ),
    )

    # useful / (useful + full_positive)
    bloom_hit_rate = expr_operator(
        Expr("rocksdb_bloom_filter_useful"),
        "/",
        expr_operator(
            Expr("rocksdb_bloom_filter_useful"),
            "+",
            Expr("rocksdb_bloom_filter_full_positive"),
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


def get_templates():
    base_metric = convert_metric("rocksdb.num-immutable-mem-table")
    return Templating(
        list=[
            Template(name="source", query="prometheus", type="datasource"),
            template(
                name="instance",
                query=f"label_values({base_metric}, instance)",
                data_source="${source}",
                hide=0,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="cf",
                query=f"label_values({base_metric}, cf)",
                data_source="${source}",
                hide=0,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
        ]
    )


dashboard = Dashboard(
    "RocksDB Metrics",
    templating=get_templates(),
    refresh="1m",
    panels=[
        memtable_metrics(),
        sst_metrics(),
        blob_db_metrics(),
        flush_metrics(),
        compaction_metrics(),
        block_cache_metrics(),
    ],
    annotations=Annotations(),
    uid="cdleji62a1b0gb",
    version=9,
    schemaVersion=14,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    timezone="browser",
).auto_panel_ids()

if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else None
    with open(output, "w") if output else sys.stdout as stream:
        _gen.write_dashboard(dashboard, stream)
