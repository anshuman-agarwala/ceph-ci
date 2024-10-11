#include <memory>
#include <string>
#include <vector>

#include "common/Formatter.h"

#include "include/types.h"

class JSONObj;

namespace ceph {

  class Formatter;

  namespace io_exerciser {
    namespace json {
      class JSONStructure
      {
        public:
          JSONStructure(std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          JSONStructure(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          virtual ~JSONStructure();

          std::string encode_json();
          virtual void decode_json(JSONObj* obj)=0;
          virtual void dump() const = 0;

        protected:
          std::shared_ptr<ceph::Formatter> formatter;

        private:
          std::ostringstream oss;
      };

      class OSDMapRequest : public JSONStructure
      {
        public:
          OSDMapRequest(const std::string& pool_name, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          OSDMapRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix = "osd map";
          std::string pool;
          std::string format = "json";

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class OSDMapReply : public JSONStructure
      {
        public:
          OSDMapReply(JSONObj *obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          epoch_t epoch;
          std::string pool;
          uint64_t pool_id;
          std::string objname;
          std::string raw_pgid;
          std::string pgid;
          std::vector<int> up;
          int up_primary;
          std::vector<int> acting;
          int acting_primary;

          void decode_json(JSONObj *obj);
          void dump() const;
      };

      class OSDECProfileSetRequest : public JSONStructure
      {
        public:
          OSDECProfileSetRequest(const std::string& name, std::vector<std::string> profile,
                                 std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          OSDECProfileSetRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix = "osd erasure-code-profile set";
          std::string name;
          std::vector<std::string> profile;

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class OSDECPoolCreateRequest : public JSONStructure
      {
        public:
          OSDECPoolCreateRequest(const std::string& pool, const std::string& erasure_code_profile,
                                 std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          OSDECPoolCreateRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix = "osd pool create";
          std::string pool;
          std::string pool_type = "erasure";
          int pg_num = 8;
          int pgp_num = 8;
          std::string erasure_code_profile;

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class OSDSetRequest : public JSONStructure
      {
        public:
          OSDSetRequest(const std::string& key, const std::optional<bool>& yes_i_really_mean_it = std::nullopt,
                        std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          OSDSetRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix = "osd set";
          std::string key;
          std::optional<bool> yes_i_really_mean_it = std::nullopt;

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class BalancerOffRequest : public JSONStructure
      {
        public:
          BalancerOffRequest(std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          BalancerOffRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix = "balancer off";

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class BalancerStatusRequest : public JSONStructure
      {
        public:
          BalancerStatusRequest(std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          BalancerStatusRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix = "balancer status";

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class BalancerStatusReply : public JSONStructure
      {
        public:
          BalancerStatusReply(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          bool active;
          std::string last_optimization_duration;
          std::string last_optimization_started;
          std::string mode;
          bool no_optimization_needed;
          std::string optimize_result;

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class ConfigSetRequest : public JSONStructure
      {
        public:
          ConfigSetRequest(std::string who, std::string name, const std::string& value, std::optional<bool> force = std::nullopt, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          // ConfigSetRequest(std::string who, std::string name, bool value, std::optional<bool> force = std::nullopt, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          // ConfigSetRequest(std::string who, std::string name, int value, std::optional<bool> force = std::nullopt, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));
          ConfigSetRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix = "config set";
          std::string who;
          std::string name;
          std::string value;
          std::optional<bool> force;

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      enum class InjectOpType {
        Read,
        Write
      };

      class InjectECErrorRequest : public JSONStructure
      {
        public:
          InjectECErrorRequest(InjectOpType injectOpType,
                               const std::string& pool,
                               const std::string& objname,
                               int shardid,
                               std::optional<int> type,
                               std::optional<int> when,
                               std::optional<int> duration,
                               std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix;
          std::string pool;
          std::string objname;
          int shardid;
          std::optional<int> type;
          std::optional<int> when;
          std::optional<int> duration;

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };

      class InjectECClearErrorRequest : public JSONStructure
      {
        public:
          InjectECClearErrorRequest(InjectOpType injectOpType,
                                    const std::string& pool,
                                    const std::string& objname,
                                    int shardid,
                                    std::optional<int> type,
                                    std::shared_ptr<ceph::Formatter> formatter = std::make_shared<JSONFormatter>(false));

          std::string prefix;
          std::string pool;
          std::string objname;
          int shardid;
          std::optional<int> type;

          void decode_json(JSONObj* obj) override;
          void dump() const override;
      };
    }
  }
}