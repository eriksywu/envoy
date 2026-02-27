# Design: Per-xDS-Resource `created_timestamp` for Envoy Stats

## Status: Decisions Finalized

-----

## Problem

Envoy currently uses a single process start time as the temporal anchor for all cumulative metrics, regardless of when the underlying xDS resource was discovered. This is semantically incorrect: a cluster discovered via CDS 6 hours into the process gets the same start time as a static cluster loaded at boot. This distorts rate calculations and masks counter resets.

The Prometheus protobuf exposition format supports a `created_timestamp` field on `Counter`, `Summary`, and `Histogram` messages, but Envoy never populates it. The OTel sink similarly uses a uniform `proxy_start_time_ns_` for all cumulative datapoints’ `start_time_unix_nano`.

## Goal

Track the wall-clock time each xDS resource (cluster, route configuration, listener) is first seen by this Envoy instance, and expose it as `created_timestamp` in the Prometheus protobuf exposition format.

**Scope of this PR:**

- `ResourceTimestampRegistry` implementation
- Integration with CDS, RDS, LDS resource lifecycle
- Prometheus protobuf exposition (`/stats?format=prometheus` with protobuf content negotiation)

**Explicit non-goals for this PR:**

- OTel sink `start_time_unix_nano` per-resource (separate follow-up PR)
- Prometheus text exposition `_created` lines (not included; protobuf only)
- EDS per-endpoint timestamps (follow-up; cluster-level only for now)
- Hot restart timestamp preservation (child resets all to its own start time)

-----

## Design Decisions

|Decision            |Choice                                       |Rationale                                                                      |
|--------------------|---------------------------------------------|-------------------------------------------------------------------------------|
|Hot restart behavior|Reset to child start time                    |Simpler, matches current OTel/Prom behavior, no RPC transfer needed            |
|Prometheus format   |Protobuf only                                |Lower risk; most scrapers negotiate protobuf anyway                            |
|Resource granularity|Cluster, listener, route config              |Endpoints are 10-100x more entries; follow-up                                  |
|OTel sink changes   |Separate PR                                  |Smaller review surface per PR                                                  |
|Registry ownership  |`ServerImpl`                                 |Avoids cross-manager coupling; accessible to both xDS writers and admin readers|
|Timestamp encoding  |`uint32_t` delta-from-process-start (seconds)|4 bytes/resource, O(1) lookup, 136-year range                                  |

-----

## `ResourceTimestampRegistry`

### Interface

```cpp
// source/common/stats/resource_timestamp_registry.h

namespace Envoy {
namespace Stats {

/**
 * Tracks the first-seen wall-clock time for xDS-managed resources (clusters,
 * listeners, route configs). Used to populate Prometheus created_timestamp
 * and (in future) OTel start_time_unix_nano on a per-resource basis.
 *
 * Thread safety: writers are serialized on the main thread (xDS updates);
 * readers may call from admin handler threads. Protected by mutex.
 */
class ResourceTimestampRegistry {
public:
  explicit ResourceTimestampRegistry(int64_t process_start_epoch_seconds)
      : process_start_epoch_seconds_(process_start_epoch_seconds) {}

  /**
   * Record the first-seen time for a resource. No-op if the resource
   * has already been recorded (first-seen semantics).
   *
   * Called from xDS subscription callbacks on the main thread, or
   * during bootstrap for static resources.
   *
   * @param resource_name symbolized resource identifier (e.g., cluster name)
   * @param epoch_seconds wall-clock time in seconds since Unix epoch
   */
  void recordFirstSeen(StatName resource_name, int64_t epoch_seconds);

  /**
   * Remove a resource's timestamp. Called when a resource is removed
   * via CDS/RDS/LDS state-of-the-world replacement or delta removal.
   * If the resource is re-added later, it gets a new first-seen time.
   *
   * @param resource_name symbolized resource identifier
   */
  void remove(StatName resource_name);

  /**
   * Look up the first-seen time for a resource. Returns process start
   * time if the resource is not found (correct for server-level stats
   * and any stat not associated with a tracked resource).
   *
   * Called from admin handler threads during scrape.
   *
   * @param resource_name symbolized resource identifier
   * @return epoch seconds of first observation
   */
  int64_t getFirstSeen(StatName resource_name) const;

  /**
   * @return the process start time used as the epoch anchor and default.
   */
  int64_t processStartTime() const { return process_start_epoch_seconds_; }

private:
  const int64_t process_start_epoch_seconds_;

  mutable Thread::MutexBasicLockable lock_;
  // Value is (epoch_seconds - process_start_epoch_seconds_), stored as uint32_t.
  // Reconstruct absolute time as: process_start_epoch_seconds_ + delta.
  absl::flat_hash_map<StatName, uint32_t> deltas_ ABSL_GUARDED_BY(lock_);
};

} // namespace Stats
} // namespace Envoy
```

### Implementation

```cpp
// source/common/stats/resource_timestamp_registry.cc

void ResourceTimestampRegistry::recordFirstSeen(StatName resource_name,
                                                 int64_t epoch_seconds) {
  Thread::LockGuard lock(lock_);
  // insert-if-absent: no-op if already present (first-seen semantics)
  deltas_.try_emplace(resource_name,
                      static_cast<uint32_t>(epoch_seconds - process_start_epoch_seconds_));
}

void ResourceTimestampRegistry::remove(StatName resource_name) {
  Thread::LockGuard lock(lock_);
  deltas_.erase(resource_name);
}

int64_t ResourceTimestampRegistry::getFirstSeen(StatName resource_name) const {
  Thread::LockGuard lock(lock_);
  auto it = deltas_.find(resource_name);
  if (it == deltas_.end()) {
    return process_start_epoch_seconds_;
  }
  return process_start_epoch_seconds_ + static_cast<int64_t>(it->second);
}
```

### StatName Key Lifetime

The `StatName` keys in `deltas_` are non-owning pointers into the symbol table’s storage. This is safe because:

1. The resource’s stats (which hold refcounts on those symbols) outlive their entries in this map — a resource’s stats are cleaned up at the same time as (or after) the `remove()` call.
1. The registry itself is destroyed during server shutdown, after the cluster/listener managers have cleaned up.

If this assumption is found to be fragile during implementation, the fallback is to use `StatNameStorage` (owning copies) as keys instead, at the cost of one additional symbol table allocation per resource.

-----

## Ownership and Wiring

```
ServerImpl
├── owns ResourceTimestampRegistry (constructed with process start time)
│
├── ClusterManagerImpl (receives registry ref)
│   └── CDS subscription callback → registry.recordFirstSeen / registry.remove
│
├── ListenerManagerImpl (receives registry ref)
│   └── LDS subscription callback → registry.recordFirstSeen / registry.remove
│
├── RouteConfigProviderManager (receives registry ref)
│   └── RDS subscription callback → registry.recordFirstSeen / registry.remove
│
└── AdminImpl
    └── StatsHandler / PrometheusStatsFormatter (receives registry ref)
        └── scrape path → registry.getFirstSeen
```

The registry is constructed in `ServerImpl::initialize()` immediately after capturing the process start time, before the cluster manager and listener manager are created. It’s passed by const reference to the admin handler (read-only) and by mutable reference to the managers (read-write).

-----

## Integration: Recording Timestamps

### Static Resources (Bootstrap)

In `ClusterManagerImpl` initialization, after loading `static_resources.clusters`:

```cpp
for (const auto& cluster : bootstrap.static_resources().clusters()) {
    // ... existing cluster creation ...
    timestamp_registry_.recordFirstSeen(
        cluster_info->statsScope().statName(),  // symbolized cluster name
        timestamp_registry_.processStartTime());
}
```

Similarly in `ListenerManagerImpl` for static listeners, and in route config providers for inline route configs.

### Dynamic Resources (xDS)

In `CdsApiImpl::onConfigUpdate()` for state-of-the-world CDS:

```cpp
// New/updated clusters from this response:
for (const auto& resource : added_resources) {
    // ... existing cluster creation ...
    timestamp_registry_.recordFirstSeen(cluster_stat_name, current_epoch_seconds);
}

// Clusters absent from this SotW response are implicitly removed.
// The existing removal logic already iterates removed clusters:
for (const auto& removed_cluster : removed_clusters) {
    timestamp_registry_.remove(removed_cluster_stat_name);
}
```

For delta CDS, the added/removed resources are explicit in the `DeltaDiscoveryResponse`.

The same pattern applies to LDS (`ListenerManagerImpl::onConfigUpdate`) and RDS (`RdsRouteConfigSubscription::onConfigUpdate`).

-----

## Integration: Prometheus Protobuf Exposition

### Resolving Resource Name from a Metric

At scrape time, the code maps a `Stats::Metric` to its owning resource using the metric’s tags. The well-known tag names determine which tag to use:

|Stat prefix pattern|Lookup tag                      |Example value                   |
|-------------------|--------------------------------|--------------------------------|
|`cluster.*`        |`envoy.cluster_name`            |`my_service`                    |
|`listener.*`       |`envoy.listener_address`        |`0.0.0.0_8080`                  |
|`http.*`           |`envoy.http_conn_manager_prefix`|`ingress_http`                  |
|`vhost.*`          |`envoy.virtual_host`            |`my_vhost`                      |
|(no match)         |—                               |Falls back to process start time|

This resolution is implemented as a helper function:

```cpp
// In prometheus_stats.cc or a shared utility

StatName resolveResourceName(const Stats::Metric& metric,
                             const StatNameSet& well_known_tags) {
  StatName result;
  // Ordered by expected frequency to short-circuit early
  static const std::array<absl::string_view, 4> resource_tags = {
      "envoy.cluster_name",
      "envoy.listener_address",
      "envoy.http_conn_manager_prefix",
      "envoy.virtual_host",
  };

  metric.iterateTagStatNames([&](StatName tag_name, StatName tag_value) -> bool {
    for (const auto& candidate : resource_tags) {
      StatName candidate_stat_name = well_known_tags.getBuiltin(candidate, StatName());
      if (!candidate_stat_name.empty() && tag_name == candidate_stat_name) {
        result = tag_value;
        return false;  // stop iteration
      }
    }
    return true;  // continue
  });

  return result;
}
```

This uses `iterateTagStatNames` which operates on symbolized names — no string allocation, no symbol table lock.

### Setting `created_timestamp` in the Protobuf Output

In `ProtobufFormat::generateNumericOutput()`, for counters only:

```cpp
for (const auto* metric : metrics) {
  auto* prom_metric = metric_family.add_metric();
  addLabelsToMetric(prom_metric, metric->tags());

  if (type == io::prometheus::client::MetricType::COUNTER) {
    auto* counter = prom_metric->mutable_counter();
    counter->set_value(metric->value());

    // Populate created_timestamp
    StatName resource = resolveResourceName(*metric, well_known_tags);
    int64_t first_seen = timestamp_registry.getFirstSeen(resource);
    auto* ct = counter->mutable_created_timestamp();
    ct->set_seconds(first_seen);
    ct->set_nanos(0);
  } else {
    auto* gauge = prom_metric->mutable_gauge();
    gauge->set_value(metric->value());
  }
}
```

Similarly for `generateHistogramOutput()` and `generateSummaryOutput()`, which set the `created_timestamp` on the `Histogram` and `Summary` proto messages respectively.

**Note:** `Gauge` does not have a `created_timestamp` field in the Prometheus data model, so no change is needed for gauges.

-----

## Concurrency

- **Writers** (`recordFirstSeen`, `remove`): Main thread only, during xDS config updates. Already serialized by the xDS subscription machinery.
- **Readers** (`getFirstSeen`): Admin handler threads during scrape. Frequency: every 15-30s per Prometheus scraper.

A `MutexBasicLockable` is appropriate. The lock is held for the duration of a hash map lookup (~tens of nanoseconds). At scrape time, the lock will be acquired once per metric that has a resource tag — for 10k clusters × ~3-5 counters/histograms per cluster = ~30-50k lock acquisitions per scrape, completing in well under 1ms total.

-----

## Memory Budget

|Component                                 |Per resource |10k clusters|
|------------------------------------------|-------------|------------|
|`StatName` key (pointer into symbol table)|~16 bytes    |160 KB      |
|`uint32_t` delta value                    |4 bytes      |40 KB       |
|`absl::flat_hash_map` overhead            |~32 bytes    |320 KB      |
|**Total**                                 |**~52 bytes**|**~520 KB** |

This is ~1% of the existing per-cluster stats memory footprint.

-----

## Test Plan

### Unit Tests

1. **`ResourceTimestampRegistry` basic operations**: record, lookup, remove, re-add produces new timestamp, lookup miss returns process start time.
1. **First-seen semantics**: second `recordFirstSeen` for same name is a no-op.
1. **`resolveResourceName` helper**: correctly extracts resource name from metrics with different tag combinations, returns empty for untagged metrics.

### Integration Tests

1. **Memory golden test**: Update `test/integration/stats_integration_test.cc` expected per-cluster memory to account for registry overhead (~52 bytes/cluster).
1. **Prometheus protobuf output**: Scrape the admin endpoint with protobuf content negotiation. Verify:
- Counters for a **static cluster** have `created_timestamp.seconds == process_start_time`
- After a CDS update adding a new cluster, counters for that cluster have `created_timestamp.seconds > process_start_time`
- After CDS removes and re-adds a cluster, the `created_timestamp` reflects the re-add time
- Gauges do NOT have `created_timestamp`
- Histograms and summaries DO have `created_timestamp`
1. **Prometheus text format**: Verify no `_created` lines appear (out of scope).
1. **Server-level stats**: Stats like `server.live` have `created_timestamp == process_start_time` (fallback path).

### Concurrency Test

1. **Parallel scrape during config update**: Issue a CDS update while an admin scrape is in progress. Verify no crashes, no torn reads, and the scrape completes with consistent timestamps.

-----

## Future Work (Separate PRs)

1. **OTel sink**: Replace uniform `cumulative_start_time_ns_` with per-resource lookups using the same `ResourceTimestampRegistry`.
1. **EDS endpoint timestamps**: Extend the registry to track per-endpoint first-seen times for host-level stats.
1. **Text exposition**: Emit `_created` lines in OpenMetrics text format.
1. **Hot restart preservation**: Optionally transfer timestamps from parent to child via the existing RPC mechanism.