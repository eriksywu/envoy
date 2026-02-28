#pragma once

#include <cstdint>
#include <string>

#include "source/common/common/thread.h"

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"

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
   * @param resource_name resource identifier (e.g., cluster name)
   * @param epoch_seconds wall-clock time in seconds since Unix epoch
   */
  void recordFirstSeen(absl::string_view resource_name, int64_t epoch_seconds);

  /**
   * Remove a resource's timestamp. Called when a resource is removed
   * via CDS/RDS/LDS state-of-the-world replacement or delta removal.
   * If the resource is re-added later, it gets a new first-seen time.
   *
   * @param resource_name resource identifier
   */
  void remove(absl::string_view resource_name);

  /**
   * Look up the first-seen time for a resource. Returns 0 if the resource
   * is not found, allowing callers to fall back to process start time.
   *
   * @param resource_name resource identifier
   * @return epoch seconds of first observation, or 0 if not found
   */
  int64_t getFirstSeen(absl::string_view resource_name) const;

  /**
   * @return the process start time used as the default for untracked resources.
   */
  int64_t processStartTime() const { return process_start_epoch_seconds_; }

private:
  const int64_t process_start_epoch_seconds_;

  mutable Thread::MutexBasicLockable lock_;
  absl::flat_hash_map<std::string, int64_t> timestamps_ ABSL_GUARDED_BY(lock_);
};

} // namespace Stats
} // namespace Envoy
