//! OpenTelemetry metrics and traces (REQ-0034).
//!
//! Metrics are pushed over OTLP/HTTP every 15 s (REQ-0034 AC1). Analytics replication lag is
//! exported per shard both in seconds and in log entries (AC2), from a registry the analytics
//! replicas update. The exporter uses plain HTTP so no TLS library is linked here; encrypted
//! export comes with the aws-lc TLS stack (TASK-0033).

// reqforge: implements REQ-0034

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider, ObservableGauge};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Export interval required by REQ-0034 AC1.
pub const EXPORT_INTERVAL: Duration = Duration::from_secs(15);

pub const METER_NAME: &str = "dscore-server";

pub mod names {
    pub const REQUEST_DURATION: &str = "request_duration_seconds";
    pub const COMMIT_DURATION: &str = "txn_commit_duration_seconds";
    pub const WAL_FSYNC_DURATION: &str = "wal_fsync_duration_seconds";
    pub const TXN_COMMITS: &str = "txn_commits";
    pub const TXN_ABORTS: &str = "txn_aborts";
    pub const LEADER_CHANGES: &str = "raft_leader_changes";
    pub const ANALYTICS_LAG_SECONDS: &str = "analytics_replication_lag_seconds";
    pub const ANALYTICS_LAG_ENTRIES: &str = "analytics_replication_lag_entries";
}

/// Attribute naming the shard (range) a lag sample belongs to.
pub const RANGE_ID: &str = "range_id";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LagSample {
    pub seconds: f64,
    pub entries: u64,
}

/// Latest analytics replication lag per range, read by the lag gauges at each export.
#[derive(Debug, Clone, Default)]
pub struct AnalyticsLag {
    ranges: Arc<RwLock<HashMap<u64, LagSample>>>,
}

impl AnalyticsLag {
    pub fn record(&self, range_id: u64, seconds: f64, entries: u64) {
        self.ranges
            .write()
            .expect("lag lock poisoned")
            .insert(range_id, LagSample { seconds, entries });
    }

    /// Stop reporting a range, e.g. after its analytics replica is removed.
    pub fn remove(&self, range_id: u64) {
        self.ranges
            .write()
            .expect("lag lock poisoned")
            .remove(&range_id);
    }

    pub fn snapshot(&self) -> Vec<(u64, LagSample)> {
        let mut v: Vec<_> = self
            .ranges
            .read()
            .expect("lag lock poisoned")
            .iter()
            .map(|(k, s)| (*k, *s))
            .collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }
}

/// The instruments REQ-0034 lists. Abort rate is `txn_aborts / (txn_commits + txn_aborts)`.
pub struct Metrics {
    pub request_duration: Histogram<f64>,
    pub commit_duration: Histogram<f64>,
    pub wal_fsync_duration: Histogram<f64>,
    pub txn_commits: Counter<u64>,
    pub txn_aborts: Counter<u64>,
    pub leader_changes: Counter<u64>,
    pub analytics_lag: AnalyticsLag,
    _lag_gauges: (ObservableGauge<f64>, ObservableGauge<u64>),
}

impl Metrics {
    pub fn new(meter: &Meter) -> Self {
        let analytics_lag = AnalyticsLag::default();
        let seconds_src = analytics_lag.clone();
        let entries_src = analytics_lag.clone();
        let lag_seconds = meter
            .f64_observable_gauge(names::ANALYTICS_LAG_SECONDS)
            .with_unit("s")
            .with_description("Analytics replica replication lag per range, in seconds")
            .with_callback(move |o| {
                for (range, s) in seconds_src.snapshot() {
                    o.observe(s.seconds, &[KeyValue::new(RANGE_ID, range as i64)]);
                }
            })
            .build();
        let lag_entries = meter
            .u64_observable_gauge(names::ANALYTICS_LAG_ENTRIES)
            .with_unit("{entry}")
            .with_description("Analytics replica replication lag per range, in Raft log entries")
            .with_callback(move |o| {
                for (range, s) in entries_src.snapshot() {
                    o.observe(s.entries, &[KeyValue::new(RANGE_ID, range as i64)]);
                }
            })
            .build();
        Self {
            request_duration: meter
                .f64_histogram(names::REQUEST_DURATION)
                .with_unit("s")
                .with_description("Client request latency")
                .build(),
            commit_duration: meter
                .f64_histogram(names::COMMIT_DURATION)
                .with_unit("s")
                .with_description("Transaction commit latency")
                .build(),
            wal_fsync_duration: meter
                .f64_histogram(names::WAL_FSYNC_DURATION)
                .with_unit("s")
                .with_description("Raft log (WAL) fsync latency")
                .build(),
            txn_commits: meter
                .u64_counter(names::TXN_COMMITS)
                .with_unit("{transaction}")
                .with_description("Committed transactions")
                .build(),
            txn_aborts: meter
                .u64_counter(names::TXN_ABORTS)
                .with_unit("{transaction}")
                .with_description("Aborted transactions, by reason")
                .build(),
            leader_changes: meter
                .u64_counter(names::LEADER_CHANGES)
                .with_unit("{change}")
                .with_description("Raft leader changes observed on this node")
                .build(),
            analytics_lag,
            _lag_gauges: (lag_seconds, lag_entries),
        }
    }
}

/// Process-wide telemetry: providers plus the instruments. Call `shutdown` before exit so the
/// last interval is flushed.
pub struct Telemetry {
    meter_provider: SdkMeterProvider,
    tracer_provider: Option<SdkTracerProvider>,
    pub metrics: Metrics,
}

impl Telemetry {
    /// Metrics and traces over OTLP/HTTP to `endpoint` (e.g. `http://collector:4318`).
    pub fn otlp(endpoint: &str, node_id: u64) -> Result<Self, TelemetryError> {
        let base = endpoint.trim_end_matches('/');
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{base}/v1/metrics"))
            .build()
            .map_err(|e| TelemetryError(e.to_string()))?;
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{base}/v1/traces"))
            .build()
            .map_err(|e| TelemetryError(e.to_string()))?;
        let resource = resource(node_id);
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(metric_exporter)
                    .with_interval(EXPORT_INTERVAL)
                    .build(),
            )
            .with_resource(resource.clone())
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_resource(resource)
            .build();
        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        Ok(Self::from_provider(meter_provider, Some(tracer_provider)))
    }

    /// Wrap an existing meter provider (tests, or embedding in another process).
    pub fn from_provider(
        meter_provider: SdkMeterProvider,
        tracer_provider: Option<SdkTracerProvider>,
    ) -> Self {
        let metrics = Metrics::new(&meter_provider.meter(METER_NAME));
        Self {
            meter_provider,
            tracer_provider,
            metrics,
        }
    }

    pub fn force_flush(&self) -> Result<(), TelemetryError> {
        self.meter_provider
            .force_flush()
            .map_err(|e| TelemetryError(e.to_string()))
    }

    pub fn shutdown(self) -> Result<(), TelemetryError> {
        if let Some(t) = self.tracer_provider {
            t.shutdown().map_err(|e| TelemetryError(e.to_string()))?;
        }
        self.meter_provider
            .shutdown()
            .map_err(|e| TelemetryError(e.to_string()))
    }
}

pub fn resource(node_id: u64) -> Resource {
    Resource::builder()
        .with_service_name("dscore-server")
        .with_attribute(KeyValue::new("dscore.node_id", node_id as i64))
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryError(pub String);

impl fmt::Display for TelemetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "telemetry: {}", self.0)
    }
}

impl std::error::Error for TelemetryError {}
