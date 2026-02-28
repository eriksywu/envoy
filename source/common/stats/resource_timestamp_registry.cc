#include "source/common/stats/resource_timestamp_registry.h"

namespace Envoy {
namespace Stats {

void ResourceTimestampRegistry::recordFirstSeen(absl::string_view resource_name,
                                                int64_t epoch_seconds) {
  Thread::LockGuard lock(lock_);
  timestamps_.try_emplace(std::string(resource_name), epoch_seconds);
}

void ResourceTimestampRegistry::remove(absl::string_view resource_name) {
  Thread::LockGuard lock(lock_);
  timestamps_.erase(resource_name);
}

int64_t ResourceTimestampRegistry::getFirstSeen(absl::string_view resource_name) const {
  Thread::LockGuard lock(lock_);
  auto it = timestamps_.find(resource_name);
  if (it == timestamps_.end()) {
    return 0;
  }
  return it->second;
}

} // namespace Stats
} // namespace Envoy
