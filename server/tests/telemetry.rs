//! STORY-0027 E1: analytics replication lag is exported per shard in seconds and log entries.

use dscore_server::telemetry::{EXPORT_INTERVAL, RANGE_ID, Telemetry, names};
use opentelemetry::Value;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

fn telemetry() -> (Telemetry, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    (Telemetry::from_provider(provider, None), exporter)
}

fn find<'a>(batches: &'a [ResourceMetrics], name: &str) -> &'a Metric {
    batches
        .iter()
        .flat_map(|r| r.scope_metrics())
        .flat_map(|s| s.metrics())
        .filter(|m| m.name() == name)
        .last()
        .unwrap_or_else(|| panic!("metric {name} was not exported"))
}

fn range_of(attrs: impl Iterator<Item = impl std::borrow::Borrow<opentelemetry::KeyValue>>) -> i64 {
    attrs
        .filter_map(|kv| {
            let kv = kv.borrow();
            match (&kv.key.as_str(), &kv.value) {
                (&RANGE_ID, Value::I64(r)) => Some(*r),
                _ => None,
            }
        })
        .next()
        .expect("lag sample has no range_id attribute")
}

// reqforge: verifies REQ-0034#AC2
#[test]
fn analytics_lag_units_seconds_and_entries_per_shard() {
    let (t, exporter) = telemetry();
    t.metrics.analytics_lag.record(7, 2.5, 120);
    t.metrics.analytics_lag.record(9, 0.0, 0);
    t.force_flush().unwrap();
    let batches = exporter.get_finished_metrics().unwrap();

    let seconds = find(&batches, names::ANALYTICS_LAG_SECONDS);
    assert_eq!(seconds.unit(), "s");
    let AggregatedMetrics::F64(MetricData::Gauge(g)) = seconds.data() else {
        panic!("{} is not an f64 gauge", names::ANALYTICS_LAG_SECONDS)
    };
    let mut got: Vec<(i64, f64)> = g
        .data_points()
        .map(|p| (range_of(p.attributes()), p.value()))
        .collect();
    got.sort_by_key(|(r, _)| *r);
    assert_eq!(got, vec![(7, 2.5), (9, 0.0)]);

    let entries = find(&batches, names::ANALYTICS_LAG_ENTRIES);
    assert_eq!(entries.unit(), "{entry}");
    let AggregatedMetrics::U64(MetricData::Gauge(g)) = entries.data() else {
        panic!("{} is not a u64 gauge", names::ANALYTICS_LAG_ENTRIES)
    };
    let mut got: Vec<(i64, u64)> = g
        .data_points()
        .map(|p| (range_of(p.attributes()), p.value()))
        .collect();
    got.sort_by_key(|(r, _)| *r);
    assert_eq!(got, vec![(7, 120), (9, 0)]);

    // A removed range stops being reported at the next export.
    exporter.reset();
    t.metrics.analytics_lag.remove(9);
    t.force_flush().unwrap();
    let batches = exporter.get_finished_metrics().unwrap();
    let AggregatedMetrics::U64(MetricData::Gauge(g)) =
        find(&batches, names::ANALYTICS_LAG_ENTRIES).data()
    else {
        panic!("wrong type")
    };
    assert_eq!(g.data_points().count(), 1);
}

// reqforge: verifies REQ-0034#AC1
#[test]
fn analytics_lag_units_all_listed_metrics_exported() {
    let (t, exporter) = telemetry();
    let m = &t.metrics;
    m.request_duration.record(0.004, &[]);
    m.commit_duration.record(0.012, &[]);
    m.wal_fsync_duration.record(0.001, &[]);
    m.txn_commits.add(3, &[]);
    m.txn_aborts.add(
        1,
        &[opentelemetry::KeyValue::new(
            "reason",
            "serialization_conflict",
        )],
    );
    m.leader_changes.add(1, &[]);
    m.analytics_lag.record(1, 0.5, 10);
    t.force_flush().unwrap();
    let batches = exporter.get_finished_metrics().unwrap();
    for name in [
        names::REQUEST_DURATION,
        names::COMMIT_DURATION,
        names::WAL_FSYNC_DURATION,
        names::TXN_COMMITS,
        names::TXN_ABORTS,
        names::LEADER_CHANGES,
        names::ANALYTICS_LAG_SECONDS,
        names::ANALYTICS_LAG_ENTRIES,
    ] {
        find(&batches, name);
    }
    assert_eq!(EXPORT_INTERVAL.as_secs(), 15);
}
