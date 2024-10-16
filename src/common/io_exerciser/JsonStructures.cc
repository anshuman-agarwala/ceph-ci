#include "JsonStructures.h"

#include "common/ceph_json.h"

ceph::io_exerciser::json::JSONStructure::JSONStructure(std::shared_ptr<ceph::Formatter> formatter) :
  formatter(formatter)
{
}

ceph::io_exerciser::json::JSONStructure::JSONStructure(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  formatter(formatter)
{

}

ceph::io_exerciser::json::JSONStructure::~JSONStructure()
{

}

std::string ceph::io_exerciser::json::JSONStructure::encode_json()
{
  oss.clear();

  dump();
  formatter->flush(oss);
  return oss.str();
}

ceph::io_exerciser::json::OSDMapRequest::OSDMapRequest(const std::string& pool_name, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter),
  pool(pool_name)
{

}

ceph::io_exerciser::json::OSDMapRequest::OSDMapRequest(JSONObj *obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(obj, formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::OSDMapRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("format", format, obj);
}

void ceph::io_exerciser::json::OSDMapRequest::dump() const
{
  formatter->open_object_section("OSDMapRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("format", format, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDMapReply::OSDMapReply(JSONObj *obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(obj, formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::OSDMapReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("epoch", epoch, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("pool_id", pool_id, obj);
  JSONDecoder::decode_json("objname", objname, obj);
  JSONDecoder::decode_json("raw_pgid", raw_pgid, obj);
  JSONDecoder::decode_json("pgid", pgid, obj);
  JSONDecoder::decode_json("up", up, obj);
  JSONDecoder::decode_json("up_primary", up_primary, obj);
  JSONDecoder::decode_json("acting", acting, obj);
  JSONDecoder::decode_json("acting_primary", acting_primary, obj);
}

void ceph::io_exerciser::json::OSDMapReply::dump() const
{
  formatter->open_object_section("OSDMapReply");
  ::encode_json("epoch", epoch, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("pool_id", pool_id, formatter.get());
  ::encode_json("objname", objname, formatter.get());
  ::encode_json("raw_pgid", raw_pgid, formatter.get());
  ::encode_json("pgid", pgid, formatter.get());
  ::encode_json("up", up, formatter.get());
  ::encode_json("up_primary", up_primary, formatter.get());
  ::encode_json("acting", acting, formatter.get());
  ::encode_json("acting_primary", acting_primary, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDECProfileSetRequest::OSDECProfileSetRequest(const std::string& name, std::vector<std::string> profile, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter),
  name(name),
  profile(profile)
{

}

ceph::io_exerciser::json::OSDECProfileSetRequest::OSDECProfileSetRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::OSDECProfileSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("profile", profile, obj);
}

void ceph::io_exerciser::json::OSDECProfileSetRequest::dump() const
{
  formatter->open_object_section("OSDECProfileSetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("name", name, formatter.get());
  ::encode_json("profile", profile, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDECPoolCreateRequest::OSDECPoolCreateRequest(const std::string& pool, const std::string& erasure_code_profile, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter),
  pool(pool),
  erasure_code_profile(erasure_code_profile)
{

}

ceph::io_exerciser::json::OSDECPoolCreateRequest::OSDECPoolCreateRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::OSDECPoolCreateRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("pool_type", pool_type, obj);
  JSONDecoder::decode_json("pg_num", pg_num, obj);
  JSONDecoder::decode_json("pgp_num", pgp_num, obj);
  JSONDecoder::decode_json("erasure_code_profile", erasure_code_profile, obj);
}

void ceph::io_exerciser::json::OSDECPoolCreateRequest::dump() const
{
  formatter->open_object_section("OSDECProfileSetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("pool_type", pool_type, formatter.get());
  ::encode_json("pg_num", pg_num, formatter.get());
  ::encode_json("pgp_num", pgp_num, formatter.get());
  ::encode_json("erasure_code_profile", erasure_code_profile, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDSetRequest::OSDSetRequest(const std::string& key, const std::optional<bool>& yes_i_really_mean_it, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter),
  key(key),
  yes_i_really_mean_it(yes_i_really_mean_it)
{

}

ceph::io_exerciser::json::OSDSetRequest::OSDSetRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::OSDSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("yes_i_really_mean_it", yes_i_really_mean_it, obj);
}

void ceph::io_exerciser::json::OSDSetRequest::dump() const
{
  formatter->open_object_section("OSDSetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("key", key, formatter.get());
  ::encode_json("yes_i_really_mean_it", yes_i_really_mean_it, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::BalancerOffRequest::BalancerOffRequest(std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{

}

ceph::io_exerciser::json::BalancerOffRequest::BalancerOffRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::BalancerOffRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
}

void ceph::io_exerciser::json::BalancerOffRequest::dump() const
{
  formatter->open_object_section("BalancerOffRequest");
  ::encode_json("prefix", prefix, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::BalancerStatusRequest::BalancerStatusRequest(std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{

}

ceph::io_exerciser::json::BalancerStatusRequest::BalancerStatusRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::BalancerStatusRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
}

void ceph::io_exerciser::json::BalancerStatusRequest::dump() const
{
  formatter->open_object_section("BalancerStatusRequest");
  ::encode_json("prefix", prefix, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::BalancerStatusReply::BalancerStatusReply(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::BalancerStatusReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("active", active, obj);
  JSONDecoder::decode_json("last_optimization_duration", last_optimization_duration, obj);
  JSONDecoder::decode_json("last_optimization_started", last_optimization_started, obj);
  JSONDecoder::decode_json("mode", mode, obj);
  JSONDecoder::decode_json("no_optimization_needed", no_optimization_needed, obj);
  JSONDecoder::decode_json("optimize_result", optimize_result, obj);

}

void ceph::io_exerciser::json::BalancerStatusReply::dump() const
{
  formatter->open_object_section("BalancerStatusReply");
  ::encode_json("active", active, formatter.get());
  ::encode_json("last_optimization_duration", last_optimization_duration, formatter.get());
  ::encode_json("last_optimization_started", last_optimization_started, formatter.get());
  ::encode_json("mode", mode, formatter.get());
  ::encode_json("no_optimization_needed", no_optimization_needed, formatter.get());
  ::encode_json("optimize_result", optimize_result, formatter.get());
  formatter->close_section();
}

// ceph::io_exerciser::json::ConfigSetRequest::ConfigSetRequest(std::string who, std::string name, bool value, std::optional<bool> force, std::shared_ptr<ceph::Formatter> formatter) :
//   JSONStructure(formatter),
//   who(who),
//   name(name),
//   value(value),
//   force(force)
// {

// }

// ceph::io_exerciser::json::ConfigSetRequest::ConfigSetRequest(std::string who, std::string name, int value, std::optional<bool> force, std::shared_ptr<ceph::Formatter> formatter) :
//   JSONStructure(formatter),
//   who(who),
//   name(name),
//   value(value),
//   force(force)
// {

// }

ceph::io_exerciser::json::ConfigSetRequest::ConfigSetRequest(std::string who, std::string name, const std::string& value, std::optional<bool> force, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter),
  who(who),
  name(name),
  value(value),
  force(force)
{

}

ceph::io_exerciser::json::ConfigSetRequest::ConfigSetRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter)
{
  decode_json(obj);
}

void ceph::io_exerciser::json::ConfigSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("who", who, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("value", value, obj);
  JSONDecoder::decode_json("force", force, obj);
}

void ceph::io_exerciser::json::ConfigSetRequest::dump() const
{
  formatter->open_object_section("ConfigSetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("who", who, formatter.get());
  ::encode_json("name", name, formatter.get());
  ::encode_json("value", value, formatter.get());
  ::encode_json("force", force, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::InjectECErrorRequest
  ::InjectECErrorRequest(InjectOpType injectOpType,
                         const std::string& pool,
                         const std::string& objname,
                         int shardid,
                         std::optional<int> type,
                         std::optional<int> when,
                         std::optional<int> duration,
                         std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter),
  pool(pool),
  objname(objname),
  shardid(shardid),
  type(type),
  when(when),
  duration(duration)
{
  switch(injectOpType)
  {
    case InjectOpType::Read:
      prefix = "injectecreaderr";
      break;
    case InjectOpType::Write:
      prefix = "injectecwriteerr";
      break;
    default:
      ceph_abort(); // Unsupported OP type
  }
}

void ceph::io_exerciser::json::InjectECErrorRequest::dump() const
{
  formatter->open_object_section("InjectECErrorRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("objname", objname, formatter.get());
  ::encode_json("shardid", shardid, formatter.get());
  ::encode_json("type", type, formatter.get());
  ::encode_json("when", when, formatter.get());
  ::encode_json("duration", duration, formatter.get());
  formatter->close_section();
}

void ceph::io_exerciser::json::InjectECErrorRequest::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("objname", objname, obj);
  JSONDecoder::decode_json("shardid", shardid, obj);
  JSONDecoder::decode_json("type", type, obj);
  JSONDecoder::decode_json("when", when, obj);
  JSONDecoder::decode_json("duration", duration, obj);
}



ceph::io_exerciser::json::InjectECClearErrorRequest
  ::InjectECClearErrorRequest(InjectOpType injectOpType,
                              const std::string& pool,
                              const std::string& objname,
                              int shardid,
                              std::optional<int> type,
                              std::shared_ptr<ceph::Formatter> formatter) :
  JSONStructure(formatter),
  pool(pool),
  objname(objname),
  shardid(shardid),
  type(type)
{
  switch(injectOpType)
  {
    case InjectOpType::Read:
      prefix = "injectecclearreaderr";
      break;
    case InjectOpType::Write:
      prefix = "injectecclearwriteerr";
      break;
    default:
      ceph_abort(); // Unsupported OP type
  }
}

void ceph::io_exerciser::json::InjectECClearErrorRequest::dump() const
{
  formatter->open_object_section("InjectECErrorRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("objname", objname, formatter.get());
  ::encode_json("shardid", shardid, formatter.get());
  ::encode_json("type", type, formatter.get());
  formatter->close_section();
}

void ceph::io_exerciser::json::InjectECClearErrorRequest::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("objname", objname, obj);
  JSONDecoder::decode_json("shardid", shardid, obj);
  JSONDecoder::decode_json("type", type, obj);
}