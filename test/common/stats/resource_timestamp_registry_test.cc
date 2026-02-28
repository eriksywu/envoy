#include "source/common/stats/resource_timestamp_registry.h"

#include "gtest/gtest.h"

namespace Envoy {
namespace Stats {
namespace {

class ResourceTimestampRegistryTest : public testing::Test {
protected:
  ResourceTimestampRegistryTest() : registry_(1000) {} // process start at epoch 1000

  ResourceTimestampRegistry registry_;
};

TEST_F(ResourceTimestampRegistryTest, ProcessStartTime) {
  EXPECT_EQ(1000, registry_.processStartTime());
}

TEST_F(ResourceTimestampRegistryTest, RecordAndLookup) {
  registry_.recordFirstSeen("cluster_a", 1050);
  EXPECT_EQ(1050, registry_.getFirstSeen("cluster_a"));
}

TEST_F(ResourceTimestampRegistryTest, LookupMissReturnsZero) {
  EXPECT_EQ(0, registry_.getFirstSeen("nonexistent"));
}

TEST_F(ResourceTimestampRegistryTest, FirstSeenSemanticsNoOp) {
  registry_.recordFirstSeen("cluster_a", 1050);
  registry_.recordFirstSeen("cluster_a", 2000); // should be no-op
  EXPECT_EQ(1050, registry_.getFirstSeen("cluster_a"));
}

TEST_F(ResourceTimestampRegistryTest, RemoveAndReAdd) {
  registry_.recordFirstSeen("cluster_a", 1050);
  registry_.remove("cluster_a");
  EXPECT_EQ(0, registry_.getFirstSeen("cluster_a"));

  // Re-adding after removal gets a new timestamp.
  registry_.recordFirstSeen("cluster_a", 2000);
  EXPECT_EQ(2000, registry_.getFirstSeen("cluster_a"));
}

TEST_F(ResourceTimestampRegistryTest, RemoveNonexistent) {
  // Should not crash.
  registry_.remove("nonexistent");
}

TEST_F(ResourceTimestampRegistryTest, MultipleResources) {
  registry_.recordFirstSeen("cluster_a", 1050);
  registry_.recordFirstSeen("listener_b", 1100);
  registry_.recordFirstSeen("route_c", 1200);

  EXPECT_EQ(1050, registry_.getFirstSeen("cluster_a"));
  EXPECT_EQ(1100, registry_.getFirstSeen("listener_b"));
  EXPECT_EQ(1200, registry_.getFirstSeen("route_c"));
}

} // namespace
} // namespace Stats
} // namespace Envoy
