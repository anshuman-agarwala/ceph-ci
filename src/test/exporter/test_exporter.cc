#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/config_proxy.h"
#include <gmock/gmock.h>
#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "exporter/util.h"
#include "exporter/DaemonMetricCollector.h"

#include <regex>
#include <string>
#include <vector>
#include <utility>

typedef std::map<std::string, std::string> labels_t;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::Pointee;
using ::testing::Matcher;
using ::testing::_;
using ::testing::SetArgReferee;
using ::testing::Invoke;
using ::testing::WithArgs;
using ::testing::AtLeast;


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG);

  g_conf().set_val("exporter_sort_metrics", "true");
  g_conf().set_val("exporter_prio_limit", "5");
  common_init_finish(g_ceph_context);

  int r = RUN_ALL_TESTS();
  return r;
}

TEST(Exporter, add_fixed_name_metrics) {
    std::vector<std::string> metrics = {
      "ceph_data_sync_from_zone2-zg1-realm1_fetch_bytes",
      "ceph_data_sync_from_zone2-zg1-realm1_fetch_bytes_sum",
      "ceph_data_sync_from_zone2-zg1-realm1_poll_latency_sum",
    };
    std::vector<std::string> expected_metrics = {
      "ceph_data_sync_from_zone_fetch_bytes",
      "ceph_data_sync_from_zone_fetch_bytes_sum",
      "ceph_data_sync_from_zone_poll_latency_sum",
    };
    std::string metric_name;
    std::pair<labels_t, std::string> new_metric;
    labels_t expected_labels;
    std::string expected_metric_name;
    for (std::size_t index = 0; index < metrics.size(); ++index) {
        std::string &metric_name = metrics[index];
        DaemonMetricCollector &collector = collector_instance();
        auto new_metric = collector.add_fixed_name_metrics(metric_name);
        expected_labels = {{"source_zone", "\"zone2-zg1-realm1\""}};
        std::string expected_metric_name = expected_metrics[index];
        EXPECT_EQ(new_metric.first, expected_labels);
        ASSERT_EQ(new_metric.second, expected_metric_name);
    }

    metric_name = "ceph_data_sync_from_zone2_fetch_bytes_count";
    DaemonMetricCollector &collector = collector_instance();
    new_metric = collector.add_fixed_name_metrics(metric_name);
    expected_labels = {{"source_zone", "\"zone2\""}};
    expected_metric_name = "ceph_data_sync_from_zone_fetch_bytes_count";
    EXPECT_EQ(new_metric.first, expected_labels);
    ASSERT_TRUE(new_metric.second == expected_metric_name);
}
