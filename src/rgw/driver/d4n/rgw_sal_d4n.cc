// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_d4n.h"
#include "rgw_rest_remoted4n.h"
#include "rgw_common.h"

namespace rgw { namespace sal {

namespace net = boost::asio;

static inline Bucket* nextBucket(Bucket* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterBucket*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;
  
  return dynamic_cast<FilterObject*>(t)->get_next();
}

D4NFilterDriver::D4NFilterDriver(Driver* _next, boost::asio::io_context& io_context) : FilterDriver(_next),
                                                                                       io_context(io_context) 
{
  const auto& config_cache = g_conf().get_val<std::string>("rgw_d4n_cache_backend");


  //connOD = std::make_shared<connection>(boost::asio::make_strand(io_context));
  //connBD = std::make_shared<connection>(boost::asio::make_strand(io_context));
  connCP = std::make_shared<connection>(boost::asio::make_strand(io_context));

  std::shared_ptr<cpp_redis::client[]> conn_cpp_OD(new cpp_redis::client[g_conf()->rgw_directory_master_count]);
  std::shared_ptr<cpp_redis::client[]> conn_cpp_BD(new cpp_redis::client[g_conf()->rgw_directory_master_count]);
  //std::shared_ptr<cpp_redis::client[]> conn_cpp_CP(new cpp_redis::client[g_conf()->rgw_directory_master_count]);

  rgw::cache::Partition partition_info;
  partition_info.name = "d4n";
  partition_info.type = "read-cache";
  partition_info.size = g_conf()->rgw_d4n_l1_datacache_size;

  if (config_cache == "ssd") {
    partition_info.location = g_conf()->rgw_d4n_l1_datacache_persistent_path;
    cacheDriver = new rgw::cache::SSDDriver(partition_info);
  } else if (config_cache == "redis")  {
    partition_info.location = "RedisCache"; // Does this value make sense? -Sam
    cacheDriver = new rgw::cache::RedisDriver(io_context, partition_info);
  } 

  lsvd_cache_enabled = g_conf()->rgw_d4n_lsvd_cache_enabled;
  if (lsvd_cache_enabled == true) { //if there is an lsvd partition on this D4N cache server
    rgw::cache::Partition lsvd_partition_info;
    lsvd_partition_info.name = "lsvd";
    lsvd_partition_info.type = "read-cache";
    lsvd_partition_info.size = g_conf()->rgw_d4n_lsvd_datacache_size;
    lsvd_partition_info.location = g_conf()->rgw_d4n_lsvd_datacache_persistent_path;
    lsvdCacheDriver = new rgw::cache::LSVDDriver(lsvd_partition_info);
  }

  //objDir = new rgw::d4n::ObjectDirectory(connOD);
  //blockDir = new rgw::d4n::BlockDirectory(connBD);
  policyDriver = new rgw::d4n::PolicyDriver(connCP, cacheDriver, "lfuda");
  blockDirCpp = new rgw::d4n::RGWBlockDirectory(conn_cpp_BD);
  objectDirCpp = new rgw::d4n::RGWObjectDirectory(conn_cpp_OD);
  //objDir = new rgw::d4n::ObjectDirectory(io_context);
  //blockDir = new rgw::d4n::BlockDirectory(io_context);
  //policyDriver = new rgw::d4n::PolicyDriver(io_context, cacheDriver, "lfuda");
}

D4NFilterDriver::~D4NFilterDriver()
{
  // call cancel() on the connection's executor
  //boost::asio::dispatch(connOD->get_executor(), [c = connOD] { c->cancel(); });
  //boost::asio::dispatch(connBD->get_executor(), [c = connBD] { c->cancel(); });
  boost::asio::dispatch(connCP->get_executor(), [c = connCP] { c->cancel(); });

  if (lsvd_cache_enabled == true) {
    delete lsvdCacheDriver;
  }

  delete cacheDriver;
  //delete objDir; 
  //delete blockDir; 
  delete policyDriver;
  delete blockDirCpp; 
  delete objectDirCpp; 
}

int D4NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  
  namespace net = boost::asio;
  using boost::redis::config;

  //TODO: we need to have more redis servers. cpp_directory code is fine
  //but cache_policy needs to be updated.
  std::string address = cct->_conf->rgw_filter_address; 
  config cfg;
  cfg.addr.host = address.substr(0, address.find(":"));
  cfg.addr.port = address.substr(address.find(":") + 1, address.length());
  cfg.clientname = "D4N.Filter";
  //cfg.username = "default";

  if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
    ldpp_dout(dpp, 10) << "D4NFilterDriver::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
    return -EDESTADDRREQ;
  }

  //connOD->async_run(cfg, {}, net::consign(net::detached, connOD));
  //connBD->async_run(cfg, {}, net::consign(net::detached, connBD));
  connCP->async_run(cfg, {}, net::consign(net::detached, connCP));
  

  FilterDriver::initialize(cct, dpp);

  if (lsvd_cache_enabled == true) {
    lsvdCacheDriver->initialize(dpp);
  }

  cacheDriver->initialize(dpp);
  //objDir->init(cct, dpp);
  //blockDir->init(cct, dpp);
  policyDriver->get_cache_policy()->init(cct, dpp, io_context, next);
  blockDirCpp->init(cct);
  objectDirCpp->init(cct);

  return 0;
}

void D4NFilterDriver::register_admin_apis(RGWRESTMgr* mgr)
{
  mgr->register_resource("remoted4n", new RGWRESTMgr_RemoteD4N);
}


std::unique_ptr<User> D4NFilterDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<D4NFilterUser>(std::move(user), this);
}

const std::string D4NFilterDriver::get_name() const
{
  std::string name = "filter< D4NFilterDriver >";
  return name;
}



std::unique_ptr<Object> D4NFilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this, filter);
}

int D4NFilterBucket::create(const DoutPrefixProvider* dpp,
                            const CreateParams& params,
                            optional_yield y)
{
  return next->create(dpp, params, y);
}

int D4NFilterObject::copy_object(User* user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  rgw::d4n::CacheObjectCpp obj = rgw::d4n::CacheObjectCpp{
                                 .objName = this->get_key().get_oid(),
                                 .bucketName = src_bucket->get_name()
                               };

  if (driver->get_obj_dir_cpp()->copy(&obj, dest_object->get_name(), dest_bucket->get_name(), y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory copy method failed." << dendl;

  // Append additional metadata to attributes 
  rgw::sal::Attrs baseAttrs = this->get_attrs();
  buffer::list bl;

  bl.append(to_iso_8601(*mtime));
  baseAttrs.insert({"mtime", bl});
  bl.clear();
  
  if (version_id != NULL) { 
    bl.append(*version_id);
    baseAttrs.insert({"version_id", bl});
    bl.clear();
  }
 
  if (!etag->empty()) {
    bl.append(*etag);
    baseAttrs.insert({"etag", bl});
    bl.clear();
  }

  if (attrs_mod == rgw::sal::ATTRSMOD_REPLACE) { // Replace 
    rgw::sal::Attrs::iterator iter;

    for (const auto& pair : attrs) {
      iter = baseAttrs.find(pair.first);
    
      if (iter != baseAttrs.end()) {
        iter->second = pair.second;
      } else {
        baseAttrs.insert({pair.first, pair.second});
      }
    }
  } else if (attrs_mod == rgw::sal::ATTRSMOD_MERGE) { // Merge 
    baseAttrs.insert(attrs.begin(), attrs.end()); 
  }

  return next->copy_object(user, info, source_zone,
                           nextObject(dest_object),
                           nextBucket(dest_bucket),
                           nextBucket(src_bucket),
                           dest_placement, src_mtime, mtime,
                           mod_ptr, unmod_ptr, high_precision_time, if_match,
                           if_nomatch, attrs_mod, copy_if_newer, attrs,
                           category, olh_epoch, delete_at, version_id, tag,
                           etag, progress_cb, progress_data, dpp, y);
}

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) 
{
  //can we assume that get_obj_attrs() has been invoked before calling set_obj_attrs()
  rgw::sal::Attrs attrs;
  std::string head_oid_in_cache;
  rgw::d4n::CacheBlockCpp block;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    if (setattrs != nullptr) {
      /* Ensure setattrs and delattrs do not overlap */
      if (delattrs != nullptr) {
        for (const auto& attr : *delattrs) {
          if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
            delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
          }
        }
      }
      //if set_obj_attrs() can be called to update existing attrs, then update_attrs() need to be called
      if (auto ret = driver->get_cache_driver()->set_attrs(dpp, head_oid_in_cache, *setattrs, y); ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver set_attrs method failed with ret: " << ret << dendl;
        return ret;
      }
    } //if setattrs != nullptr

    if (delattrs != nullptr) {
      Attrs::iterator attr;
      Attrs currentattrs = this->get_attrs();

      /* Ensure all delAttrs exist */
      for (const auto& attr : *delattrs) {
        if (std::find(currentattrs.begin(), currentattrs.end(), attr) == currentattrs.end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }

      if (auto ret = driver->get_cache_driver()->delete_attrs(dpp, head_oid_in_cache, *delattrs, y); ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed with ret: " << ret << dendl;
        return ret;
      }
    } //if delattrs != nullptr
  } else {
    if (block.deleteMarker) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }

    auto ret = next->set_obj_attrs(dpp, setattrs, delattrs, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): set_obj_attrs method of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

int D4NFilterObject::get_obj_attrs_from_cache(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool found_in_cache;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  rgw::d4n::CacheBlockCpp block;
  found_in_cache = check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y);

  if (block.deleteMarker) {
    return -ENOENT;
  } else if (found_in_cache) {
    /* Set metadata locally */
    RGWObjState astate;
    RGWQuotaInfo quota_info;

    astate.obj = this->get_obj();
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): astate.obj is: " << astate.obj.key.name << dendl;
    std::string instance;
    for (auto& attr : attrs) {
      if (attr.second.length() > 0) {
        if (attr.first == "user.rgw.mtime") {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting mtime." << dendl;
          //astate.mtime = ceph::real_clock::from_double(std::stod(attr.second.c_str()));
        } else if (attr.first == "user.rgw.object_size") {
          astate.size = std::stoull(attr.second.to_str());
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting object_size to: " << astate.size << dendl;
        } else if (attr.first == "user.rgw.accounted_size") {
          astate.accounted_size = std::stoull(attr.second.to_str());
        } else if (attr.first == "user.rgw.epoch") {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting epoch." << dendl;
          astate.epoch = std::stoull(attr.second.c_str());
        } else if (attr.first == "user.rgw.version_id") {
          instance = attr.second.to_str();
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting version_id to: " << instance << dendl;
        } else if (attr.first == "user.rgw.source_zone") {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting source zone id." << dendl;
          astate.zone_short_id = static_cast<uint32_t>(std::stoul(attr.second.c_str()));
        } else {
          ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Unexpected attribute; not locally set, attr name: " << attr.first << dendl;
        }
      }//end-if
    }//end-for
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): astate.obj is: " << astate.obj.key.name << " done!" << dendl;
    this->set_obj_state(astate);
    this->set_instance(instance); //set this only after setting object state else it won't take effect
    attrs.erase("user.rgw.mtime");
    attrs.erase("user.rgw.object_size");
    attrs.erase("user.rgw.accounted_size");
    attrs.erase("user.rgw.epoch");
    /* Set attributes locally */
    auto ret = this->set_attrs(attrs);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): D4NFilterObject set_attrs method failed." << dendl;
    }
  } // if found_in_cache = true

  return found_in_cache;
}

void D4NFilterObject::set_obj_state_attrs(const DoutPrefixProvider* dpp, optional_yield y, RGWObjState& state, rgw::sal::Attrs& attrs)
{
  bufferlist bl_val;
  bl_val.append(std::to_string(state.size));
  attrs["user.rgw.object_size"] = std::move(bl_val);

  bl_val.append(std::to_string(state.epoch));
  attrs["user.rgw.epoch"] = std::move(bl_val);

  bl_val.append(std::to_string(ceph::real_clock::to_double(state.mtime)));
  attrs["user.rgw.mtime"] = std::move(bl_val);

  if(this->have_instance()) {
    bl_val.append(this->get_instance());
    attrs["user.rgw.version_id"] = std::move(bl_val);
  }

  bl_val.append(std::to_string(state.zone_short_id));
  attrs["user.rgw.source_zone"] = std::move(bl_val);

  bl_val.append(std::to_string(state.accounted_size));
  attrs["user.rgw.accounted_size"] = std::move(bl_val); // will this get updated?

  return;
}

int D4NFilterObject::calculate_version(const DoutPrefixProvider* dpp, optional_yield y, RGWObjState& state, std::string& version)
{
  //versioned objects have instance set to versionId, and get_oid() returns oid containing instance, hence using id tag as version for non versioned objects only
  if (! this->have_instance() && version.empty()) {
    auto it = state.attrset.find(RGW_ATTR_ID_TAG);
    if (it != state.attrset.end()) {
      bufferlist bl = it->second;
      version = bl.c_str();
      ldpp_dout(dpp, 20) << __func__ << " id tag version is: " << version << dendl;
    } else {
      ldpp_dout(dpp, 0) << __func__ << " Failed to find id tag" << dendl;
      return -ENOENT;
    }
  }
  bufferlist bl;
  if (this->have_instance()) {
    version = this->get_instance();
  }

  this->set_object_version(version);

  return 0;
}

int D4NFilterObject::set_head_obj_dir_entry(const DoutPrefixProvider* dpp, optional_yield y, bool is_latest_version, bool dirty)
{
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object name: " << this->get_name() << " bucket name: " << this->get_bucket()->get_name() << dendl;
  // entry that contains latest version for versioned and non-versioned objects
  int ret = -1;
  rgw::d4n::RGWBlockDirectory* blockDir = this->driver->get_block_dir_cpp();
  if (is_latest_version) {
    rgw::d4n::CacheObjectCpp object = rgw::d4n::CacheObjectCpp{
      .objName = this->get_name(),
      .bucketName = this->get_bucket()->get_name(),
      .dirty = dirty,
      .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address },
      };

    rgw::d4n::CacheBlockCpp block = rgw::d4n::CacheBlockCpp{
      .cacheObj = object,
      .blockID = 0,
      .version = this->get_object_version(),
      .size = 0,
      };

    ret = blockDir->get(&block, y);
    if (ret == -ENOENT) {
      ret = blockDir->set(&block, y);
      if (ret < 0) {
	ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
      }
    } else if (ret == 0) { // head object exists; update instead of overwrite
      block.prevVersion = {block.version, block.deleteMarker};
      block.version = this->get_object_version();
      block.deleteMarker = false;
      block.cacheObj.dirty = dirty;
      block.cacheObj.hostsList.push_back(blockDir->cct->_conf->rgw_local_cache_address);
      ret = blockDir->set(&block, y);
      if (ret < 0) {
	ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed for head object with ret: " << ret << dendl;
    }
  }

  // In case of a distributed cache - an entry corresponding to each instance will be needed to locate the head block
  // this will also be needed for deleting an object from a version enabled bucket.
  // and in that case instead of having a separate entry for an object, this entry could be used during object listing.
  if (this->have_instance()) {
    rgw::d4n::CacheObjectCpp version_object = rgw::d4n::CacheObjectCpp{
    .objName = this->get_oid(),
    .bucketName = this->get_bucket()->get_name(),
    .dirty = dirty,
    .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address }
    };

    rgw::d4n::CacheBlockCpp version_block = rgw::d4n::CacheBlockCpp{
      .cacheObj = version_object,
      .blockID = 0,
      .version = this->get_object_version(),
      .size = 0,
    };

    ret = blockDir->get(&version_block, y);
    if (ret == -ENOENT) {
      ret = blockDir->set(&version_block, y);
      if (ret < 0) {
	ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
      }
    } else if (ret == 0) { // head object exists; update instead of overwrite
      version_block.prevVersion = {version_block.version, version_block.deleteMarker};
      version_block.version = this->get_object_version();
      version_block.deleteMarker = false;
      version_block.cacheObj.dirty = dirty;
      version_block.cacheObj.hostsList.push_back(blockDir->cct->_conf->rgw_local_cache_address);

      ret = blockDir->set(&version_block, y);
      if (ret < 0) {
	ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed for head object with ret: " << ret << dendl;
    }
  }

  return ret;
}

bool D4NFilterObject::check_head_exists_in_cache_get_oid(const DoutPrefixProvider* dpp, std::string& head_oid_in_cache, rgw::sal::Attrs& attrs, rgw::d4n::CacheBlockCpp& blk, optional_yield y)
{
  rgw::d4n::RGWBlockDirectory* blockDir = this->driver->get_block_dir_cpp();
  rgw::d4n::CacheObjectCpp object = rgw::d4n::CacheObjectCpp{
        .objName = this->get_oid(), //version-enabled buckets will not have version for latest version, so this will work even when version is not provided in input
        .bucketName = this->get_bucket()->get_name(),
        };

  rgw::d4n::CacheBlockCpp block = rgw::d4n::CacheBlockCpp{
          .cacheObj = object,
          .blockID = 0,
          .size = 0
          };

  bool found_in_cache = true;
  int ret = -1;
  //if the block corresponding to head object does not exist in directory, implies it is not cached
  if ((ret = blockDir->get(&block, y)) == 0) {
    blk = block;
    if (block.deleteMarker)
      return false;

    std::string version;
    version = block.version;
    this->set_object_version(version);

    //for distributed cache-the blockHostsList can be used to determine if the head block resides on the localhost, then get the block from localhost, whether or not the block is dirty
    //can be determined using the block entry.

    //uniform name for versioned and non-versioned objects, since input for versioned objects might not contain version
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Is block dirty: " << block.cacheObj.dirty << dendl;
    if (block.cacheObj.dirty) {
      head_oid_in_cache = "D_" + get_bucket()->get_name() + "_" + version + "_" + get_name();
    } else {
      head_oid_in_cache = get_bucket()->get_name() + "_" + version + "_" + get_name();
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from cache for head obj id: " << head_oid_in_cache << dendl;
    auto ret = this->driver->get_cache_driver()->get_attrs(dpp, head_oid_in_cache, attrs, y);
    if (ret < 0) {
      found_in_cache = false;
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver get_attrs method failed." << dendl;
    }
  } else if (ret == -ENOENT) { //if blockDir->get
    found_in_cache = false;
  } else {
    found_in_cache = false;
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed, ret=" << ret << dendl;
  }

  return found_in_cache;
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  bool is_latest_version = true;
  if (this->have_instance()) {
    is_latest_version = false;
  }
  
  int ret = -1;
  if ((ret = get_obj_attrs_from_cache(dpp, y)) == -ENOENT) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): " << " object " << this->get_name() << " does not exist." << dendl;
    return -ENOENT;
  } else if (!ret) {
    /*
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_misses);
    }
    */
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from backend store." << dendl;
    auto ret = next->get_obj_attrs(y, dpp, target_obj);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to fetch attrs from backend store, ret=" << ret << dendl;
      return ret;
    }
  
    RGWObjState* state = nullptr;
    this->get_obj_state(dpp, &state, y);
    this->obj = state->obj;
    if (!state->obj.key.instance.empty()) {
      this->set_instance(state->obj.key.instance);
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): this->obj oid is: " << this->obj.key.name << "instance is: " << this->obj.key.instance << dendl;
    attrs = this->get_attrs();
    this->set_obj_state_attrs(dpp, y, *state, attrs);

    ret = calculate_version(dpp, y, *state, version);
    if (ret < 0 || version.empty()) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }

    head_oid_in_cache = this->get_bucket()->get_name() + "_" + version + "_" + this->get_name();
    if (this->driver->get_policy_driver()->get_cache_policy()->exist_key(head_oid_in_cache) > 0) {
      ret = this->driver->get_cache_driver()->set_attrs(dpp, head_oid_in_cache, attrs, y);
    } else {
      ret = this->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
      if (ret == 0) {
        bufferlist bl;
        ret = this->driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
      } else {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to evict data, ret=" << ret << dendl;
      }
    }
    if (ret == 0) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << this->get_object_version() << dendl;
      time_t creationTime = ceph::real_clock::to_time_t(this->get_mtime());
      this->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, 0, version, false, creationTime, this->get_bucket()->get_owner(), y);
      ret = set_head_obj_dir_entry(dpp, y, is_latest_version);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): failed to cache head object in cache backend, ret=" << ret << dendl;
    }
  } else {
  /*
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
    }
  */
  }
  return 0;
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) 
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  rgw::d4n::CacheBlockCpp block;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    if (auto ret = driver->get_cache_driver()->update_attrs(dpp, head_oid_in_cache, update, y); ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver update_attrs method failed with ret: " << ret << dendl;
      return ret;
    }
  } else {
    if (block.deleteMarker) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }

    auto ret = next->modify_obj_attrs(attr_name, attr_val, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): modify_obj_attrs of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  buffer::list bl;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  Attrs delattr;
  rgw::d4n::CacheBlockCpp block;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    delattr.insert({attr_name, bl});
    Attrs currentattrs = this->get_attrs();
    rgw::sal::Attrs::iterator attr = delattr.begin();

    /* Ensure delAttr exists */
    if (std::find_if(currentattrs.begin(), currentattrs.end(),
        [&](const auto& pair) { return pair.first == attr->first; }) != currentattrs.end()) {

      if (auto ret = driver->get_cache_driver()->delete_attrs(dpp, head_oid_in_cache, delattr, y); ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed with ret: " << ret << dendl;
        return ret;
      }
    }
  } else {
    if (block.deleteMarker) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }

    if (auto ret = next->delete_obj_attrs(dpp, attr_name, y); ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): delete_obj_attrs method of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }

  return 0;
}


/*
int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) 
{
  if (setattrs != NULL) {
    // Ensure setattrs and delattrs do not overlap 
    if (delattrs != NULL) {
      for (const auto& attr : *delattrs) {
        if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }
    }

    if (driver->get_cache_driver()->set_attrs(dpp, this->get_key().get_oid(), *setattrs, y) < 0)
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver set_attrs method failed." << dendl;
  }

  if (delattrs != NULL) {
    Attrs::iterator attr;
    Attrs currentattrs = this->get_attrs();

    // Ensure all delAttrs exist
    for (const auto& attr : *delattrs) {
      if (std::find(currentattrs.begin(), currentattrs.end(), attr) == currentattrs.end()) {
	delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
      }
    }

    if (driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), *delattrs, y) < 0) 
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed." << dendl;
  }

  return next->set_obj_attrs(dpp, setattrs, delattrs, y);  
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  rgw::sal::Attrs attrs;

  if (driver->get_cache_driver()->get_attrs(dpp, this->get_key().get_oid(), attrs, y) < 0) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver get_attrs method failed." << dendl;
    return next->get_obj_attrs(y, dpp, target_obj);
  } else {
    // Set metadata locally
    RGWQuotaInfo quota_info;
    RGWObjState* astate;
    std::unique_ptr<rgw::sal::User> user = this->driver->get_user(this->get_bucket()->get_owner());
    this->get_obj_state(dpp, &astate, y);
    this->obj = astate->obj;

    for (auto it = attrs.begin(); it != attrs.end(); ++it) {
      if (it->second.length() > 0) {
	if (it->first == "mtime") {
	  parse_time(it->second.c_str(), &astate->mtime);
	  attrs.erase(it->first);
	} else if (it->first == "object_size") {
	  this->set_obj_size(std::stoull(it->second.c_str()));
	  attrs.erase(it->first);
	} else if (it->first == "accounted_size") {
	  astate->accounted_size = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "epoch") {
	  astate->epoch = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "version_id") {
	  this->set_instance(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "this_zone_short_id") {
	  astate->zone_short_id = static_cast<uint32_t>(std::stoul(it->second.c_str()));
	  attrs.erase(it->first);
	} else if (it->first == "user_quota.max_size") {
	  quota_info.max_size = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "user_quota.max_objects") {
	  quota_info.max_objects = std::stoull(it->second.c_str());
	  attrs.erase(it->first);
	} else if (it->first == "max_buckets") {
	  user->set_max_buckets(std::stoull(it->second.c_str()));
	  attrs.erase(it->first);
	} else {
	  ldpp_dout(dpp, 20) << "D4N Filter: Unexpected attribute; not locally set." << dendl;
	  attrs.erase(it->first);
	}
      }
    }

    user->set_info(quota_info);
    this->set_obj_state(*astate);
   
    // Set attributes locally
    if (this->set_attrs(attrs) < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): D4NFilterObject set_attrs method failed." << dendl;
      return next->get_obj_attrs(y, dpp, target_obj);
    }
  }

  return 0;
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) 
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;

  if (driver->get_cache_driver()->update_attrs(dpp, this->get_key().get_oid(), update, y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver update_attrs method failed." << dendl;

  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);  
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  buffer::list bl;
  Attrs delattr;
  delattr.insert({attr_name, bl});
  Attrs currentattrs = this->get_attrs();
  rgw::sal::Attrs::iterator attr = delattr.begin();

  // Ensure delAttr exists
  if (std::find_if(currentattrs.begin(), currentattrs.end(),
       [&](const auto& pair) { return pair.first == attr->first; }) != currentattrs.end()) {

    if (driver->get_cache_driver()->delete_attrs(dpp, this->get_key().get_oid(), delattr, y) < 0) 
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver delete_attrs method failed." << dendl;
  } else 
    return next->delete_obj_attrs(dpp, attr_name, y);  

  return 0;
}
*/

std::unique_ptr<Object> D4NFilterDriver::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this);
}

std::unique_ptr<Writer> D4NFilterDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<D4NFilterWriter>(std::move(writer), this, obj, dpp, true, y);
}

std::unique_ptr<Object::ReadOp> D4NFilterObject::get_read_op()
{
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<D4NFilterReadOp>(std::move(r), this);
}

std::unique_ptr<Object::DeleteOp> D4NFilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<D4NFilterDeleteOp>(std::move(d), this);
}

int D4NFilterObject::D4NFilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  rgw::sal::Attrs& attrs = source->get_attrs();
  if (attrs.empty()) {
    rgw_obj obj = source->get_obj();
    auto ret = source->get_obj_attrs(y, dpp, &obj);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Error: failed to fetch attrs, ret=" << ret << dendl;
      return ret;
    }
    //get_obj_attrs() calls set_attrs() internally, hence get_attrs() can be invoked to get the latest attrs.
    attrs = source->get_attrs();
  }
  auto it = attrs.find(name);
  if (it == attrs.end()) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Attribute value NOT found for attr name= " << name << dendl;
    return next->get_attr(dpp, name, dest, y);
  }

  dest = it->second;
  return 0;
}

/*
int D4NFilterObject::D4NFilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  ldpp_dout(dpp, 20) << "AMIN: D4NFilterObject:" << __func__ << __LINE__ << dendl;
  std::string attr_value;
  rgw::d4n::CacheObjectCpp object;
  object.objName = source->get_key().get_oid();
  object.bucketName = source->get_bucket()->get_name();
  int ret = -1;
  //int ret = source->driver->get_obj_dir_cpp()->get_attr(&object, name, dest, y);
  ldpp_dout(dpp, 20) << "AMIN: D4NFilterObject:" << __func__ << __LINE__ << ": ret is: " << ret << dendl;
  if (ret < 0){
    ldpp_dout(dpp, 20) << "AMIN: D4NFilterObject:" << __func__ << __LINE__ << dendl;
    //checking backend
    return next->get_attr(dpp, name, dest, y);
  }
  ldpp_dout(dpp, 20) << "AMIN: D4NFilterObject:" << __func__ << __LINE__ << dendl;
  return 0;
}
*/

int D4NFilterObject::D4NFilterReadOp::getRemote(const DoutPrefixProvider* dpp, long long start, long long end, std::string key, std::string remoteCacheAddress, bufferlist *bl, optional_yield y)
{
  RGWAccessKey accessKey;
  std::unique_ptr<rgw::sal::User> c_user = source->driver->get_user(source->get_bucket()->get_owner());
  int ret = c_user->load_user(dpp, y);
  if (ret < 0) {
    return -EPERM;
  }
  if (c_user->get_info().access_keys.empty()) {
    return -EINVAL;
  }
  accessKey.id = c_user->get_info().access_keys.begin()->second.id;
  accessKey.key = c_user->get_info().access_keys.begin()->second.key;

  std::string bucketName = source->get_bucket()->get_name();

  bufferlist out_bl;
  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;                                                            
  Attrs object_attrs;
  D4NGetObjectCB cb(bl);

  auto sender = new RGWRESTStreamRWRequest(dpp->get_cct(), "GET", remoteCacheAddress, &cb, NULL, NULL, "", host_style);
  
  char buf[64];
  snprintf(buf, sizeof(buf), "bytes=%lld-%lld", start, end);
  extra_headers.insert(std::make_pair("RANGE", buf));

  ret = sender->send_request(dpp, accessKey, extra_headers, source->get_obj(), nullptr);
  if (ret < 0) {                                                                                      
    delete sender;                                                                                       
    return ret;                                                                                       
  }                                                                                                   
  
  ret = sender->complete_request(y);                                                            
  if (ret < 0){
    delete sender;                                                                                   
    return ret;                                                                                   
  }
  //source->set_obj_size(received_data.length());

  return 0;
}	


int D4NFilterObject::D4NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  //set a flag to show that incoming instance has no version specified
  bool is_latest_version = true;
  if (source->have_instance()) {
    is_latest_version = false; 
  }

  int ret = -1;
  if ((ret = source->get_obj_attrs_from_cache(dpp, y)) == -ENOENT) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::" << __func__ << "(): object " << source->get_name() << " does not exist." << dendl;
    return -ENOENT;
  } else if (!ret) {
    /*
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_misses);
    }
    */
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): fetching head object from backend store" << dendl;
    next->params = params;
    auto ret = next->prepare(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): next->prepare method failed, ret=" << ret << dendl;
      return ret;
    }
    if (params.part_num) {
      params.parts_count = next->params.parts_count;
      if (params.parts_count > 1) {
        ldpp_dout(dpp, 20) << __func__ << "params.part_count: " << params.parts_count << dendl;
        return 0; // d4n wont handle multipart read requests with part number for now
      }
    }

    RGWObjState* state = nullptr;
    this->source->get_obj_state(dpp, &state, y);
    attrs = source->get_attrs();
    source->set_obj_state_attrs(dpp, y, *state, attrs);

    ret = source->calculate_version(dpp, y, *state, version);
    if (ret < 0 || version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }

    bufferlist bl;
    head_oid_in_cache = source->get_bucket()->get_name() + "_" + version + "_" + source->get_name();
    ret = source->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, attrs.size(), y);
    if (ret == 0) {
      ret = source->driver->get_cache_driver()->put(dpp, head_oid_in_cache, bl, 0, attrs, y);
      if (ret == 0) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << this->source->get_object_version() << dendl;
        time_t creationTime = ceph::real_clock::to_time_t(source->get_mtime());
        source->driver->get_policy_driver()->get_cache_policy()->update(dpp, head_oid_in_cache, 0, bl.length(), version, false, creationTime, source->get_bucket()->get_owner(), y);
        ret = source->set_head_obj_dir_entry(dpp, y, is_latest_version);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
        }
        //write object to directory.
        rgw::d4n::CacheObjectCpp object = rgw::d4n::CacheObjectCpp{
          .objName = source->get_oid(),
          .bucketName = source->get_bucket()->get_name(),
          .creationTime = std::to_string(ceph::real_clock::to_double(state->mtime)),
          .dirty = false,
          .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address }
        };
        ret = source->driver->get_obj_dir_cpp()->set(&object, y);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): ObjectDirectory set method failed with err: " << ret << dendl;
          return ret;
        }
      } else {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): put for head object failed, ret=" << ret << dendl;
      }
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): failed to cache head object during eviction, ret=" << ret << dendl;
    }
  } else {
    /*
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
    }
    */
    int retDir;
    std::string localCache = g_conf()->rgw_local_cache_address;
    rgw::d4n::CacheObjectCpp object;
    object.objName = source->get_key().get_oid();
    object.bucketName = source->get_bucket()->get_name();
    retDir = source->driver->get_obj_dir_cpp()->get(&object, y);

    if (retDir == 0){
  //the object is cached some where; local or remote.
      ldpp_dout(dpp, 20) << "AMIN: D4NFilterObject:" << __func__ << ": object size is: "  << object.size << dendl;
      source->set_obj_size(object.size);
      source->set_object_attrs(object.attrs);
      source->set_object_version(object.version);
      source->set_object_dirty(object.dirty);
      source->set_creationTime(object.creationTime);

      std::string prefix;
      std::string version = object.version;
      if (version.empty()) { //for versioned objects, get_oid() returns an oid with versionId added
        prefix = source->get_bucket()->get_name() + "_" + source->get_key().get_oid();
      } else {
        prefix = source->get_bucket()->get_name() + "_" + version + "_" + source->get_key().get_oid();
      }
      source->set_prefix(prefix);
      cached_local = 1; //it is cached
      return 0;
    }
  }

  return ret;
}

void D4NFilterObject::D4NFilterReadOp::cancel() {
  aio->drain();
}

int D4NFilterObject::D4NFilterReadOp::drain(const DoutPrefixProvider* dpp, optional_yield y) {
  do{
    auto c = aio->wait();
    int r = flush(dpp, std::move(c), y);
    if (r < 0) {
      cancel();
      return r;
    }
  } while(last_part_done == false);
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::lsvdDrain(const DoutPrefixProvider* dpp, optional_yield y) {
  auto c = aio->wait();
  int r = lsvdFlush(dpp, std::move(c), y);
  if (r < 0) {
    cancel();
    return r;
  }
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, optional_yield y) {
  int r = rgw::check_for_errors(results);

  if (r < 0) {
    return r;
  }

  std::list<bufferlist> bl_list;

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  ldpp_dout(dpp, 20) << "D4NFilterObject::In flush:: " << " id is: " << completed.front().id << dendl;
  ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " empty is: " << completed.empty() << dendl;

  while (!completed.empty() && completed.front().id == offset) {
 
    auto bl = std::move(completed.front().data);

    ldpp_dout(dpp, 20) << "D4NFilterObject::flush:: calling handle_data for offset: " << offset << " bufferlist length: " << bl.length() << dendl;

    bl_list.push_back(bl);


    ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " offset is: " << offset << dendl;
    ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " first block is: " << this->first_block << dendl;
    ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " read_ofs is: " << this->read_ofs << dendl;
    ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " block len is: " << bl.length() << dendl;


    if (client_cb) {
      if (this->first_block == true){
        ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " before handle_data" << dendl;
        int r = client_cb->handle_data(bl, read_ofs, bl.length()-read_ofs);
        ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " after handle_data" << dendl;
        set_first_block(false);
        if (r < 0) {
          return r;
        }
      }
      else{
        ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " before handle_data" << dendl;
        int r = client_cb->handle_data(bl, 0, bl.length());
        ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " after handle_data" << dendl;
        if (r < 0) {
          return r;
        }
      }
    }

    auto it = blocks_info.find(offset);
    if (it != blocks_info.end()) {
      std::string version = source->get_object_version();
      std::string prefix = source->get_prefix();
      if (version.empty()) {
        version = source->get_instance();
      }
      std::pair<uint64_t, uint64_t> ofs_len_pair = it->second;
      uint64_t ofs = ofs_len_pair.first;
      uint64_t len = ofs_len_pair.second;
      bool dirty = source->get_object_dirty();
      time_t creationTime = ceph::real_clock::to_time_t(source->get_mtime());

      std::string oid_in_cache = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(len);

      source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, len, version, dirty, creationTime,  source->get_bucket()->get_owner(), y);
      blocks_info.erase(it);
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << " offset not found: " << offset << dendl;
    }
  
    if (offset == last_adjusted_ofs)
      this->last_part_done = true;

    offset += bl.length();
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::returning from flush:: " << dendl;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::lsvdFlush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, optional_yield y) {
  int r = rgw::check_for_errors(results);

  if (r < 0) {
    return r;
  }

  std::list<bufferlist> bl_list;

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  ldpp_dout(dpp, 20) << __func__ << dendl;

  while (!completed.empty() && completed.front().id == offset) {
    auto bl = std::move(completed.front().data);

    ldpp_dout(dpp, 20) << __func__ << " calling handle_data for offset: " << offset << " bufferlist length: " << bl.length() << dendl;

    bl_list.push_back(bl);
    int r = client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      return r;
    }
    offset += bl.length();
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::returning from lsvdFlush:: " << dendl;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::remoteFlush(const DoutPrefixProvider* dpp, bufferlist bl, std::string creationTime, optional_yield y)
{
    ldpp_dout(dpp, 20) << "AMIN:DEBUG" << __func__ << "(): " <<  __LINE__ << " before handle_data" << dendl;
    if (this->first_block == true){
      int r = client_cb->handle_data(bl, read_ofs, bl.length()-read_ofs);
      set_first_block(false);
      if (r < 0) {
        return r;
      }
    }
    else{
      int r = client_cb->handle_data(bl, 0, bl.length());
      if (r < 0) {
        return r;
      }
    }
    auto it = blocks_info.find(offset);
    if (it != blocks_info.end()) {
      std::string version = source->get_object_version();
      std::string prefix = source->get_prefix();
      Attrs attrs = source->get_object_attrs();
      std::pair<uint64_t, uint64_t> ofs_len_pair = it->second;
      uint64_t ofs = ofs_len_pair.first;
      uint64_t len = ofs_len_pair.second;
      bool dirty = source->get_object_dirty();

      std::string oid_in_cache = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(len);

      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " calling update for offset: " << offset << " adjusted offset: " << ofs  << " length: " << len << " oid_in_cache: " << oid_in_cache << dendl;

      bufferlist bl_rec;
      bl.begin(0).copy(len, bl_rec);
  
      rgw::d4n::RGWBlockDirectory* blockDir = source->driver->get_block_dir_cpp();
      rgw::d4n::CacheBlockCpp block;
      block.cacheObj.objName = source->get_key().get_oid();
      block.cacheObj.bucketName = source->get_bucket()->get_name();
      block.blockID = ofs;
      block.size = len;
      block.version = version;
      block.dirty = false;
      auto ret = source->driver->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, y);
      if (ret == 0) {
        ret = source->driver->get_cache_driver()->put(dpp, oid_in_cache, bl, bl.length(), attrs, y);
        if (ret == 0) {
  	  std::string objEtag = "";
          source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, len, version, dirty, std::stol(creationTime),  source->get_bucket()->get_owner(), y);
          if (blockDir->update_field(&block, "blockHosts", blockDir->cct->_conf->rgw_local_cache_address, y) < 0)
            ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory update_field method failed for hostsList." << dendl;

	  if (ofs + len == source->get_obj_size()){ //last block
	    source->driver->get_policy_driver()->get_cache_policy()->updateObj(dpp, prefix, version, dirty, source->get_obj_size(), std::stol(creationTime), source->get_bucket()->get_owner(), objEtag, y); 
    	    rgw::d4n::RGWObjectDirectory* objectDir = source->driver->get_obj_dir_cpp();
      	    rgw::d4n::CacheObjectCpp object;
  	    object.objName = source->get_key().get_oid();
  	    object.bucketName = source->get_bucket()->get_name();

            if (objectDir->update_field(&object, "objHosts", blockDir->cct->_conf->rgw_local_cache_address, y) < 0)
              ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): objectDirectory update_field method failed for hostsList." << dendl;
          }
        }
        else {
          ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed with error: " << ret << dendl;
        }
      }

      blocks_info.erase(it);
      offset += bl.length();
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << " offset not found: " << offset << dendl;
    }

  return 0;
}

int D4NFilterObject::D4NFilterReadOp::iterateLSVD(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;

  rgw::d4n::CacheObjectCpp object;
  object.objName = source->get_key().get_oid();
  object.bucketName = source->get_bucket()->get_name();
  int ret = source->driver->get_obj_dir_cpp()->get(&object, y);
  if (ret < 0){
    ldpp_dout(dpp, 10) << "ERROR: D4NFilterObject::iterateLSVD: could not get data from directory! " << dendl;
    return ret;
  }
  std::string version = object.version;

  std::string prefix;
  if (version.empty()) { //for versioned objects, get_oid() returns an oid with versionId added
    prefix = source->get_bucket()->get_name() + "_" + source->get_key().get_oid();
  } else {
    prefix = source->get_bucket()->get_name() + "_" + version + "_" + source->get_key().get_oid();
  }

  this->client_cb = cb;
  this->cb->set_client_cb(cb, dpp, &y);
  source->set_prefix(prefix);

  uint64_t len = end - ofs + 1; //this is small object, the whole data should be read in one round

  aio = rgw::make_throttle(window_size, y);

  auto completed = source->driver->get_lsvd_cache_driver()->get_async(dpp, y, aio.get(), prefix, ofs, len, 0, 0); 

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterateLSVD:: " << __func__ << "(): Info: flushing data for oid: " << prefix << dendl;
  auto r = lsvdFlush(dpp, std::move(completed), y);

  if (r < 0) {
    lsvdDrain(dpp, y);
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << r << dendl;
    return r;
  }

  return lsvdDrain(dpp, y);

}

int D4NFilterObject::D4NFilterReadOp::findLocation(const DoutPrefixProvider* dpp, rgw::d4n::CacheBlockCpp *block, optional_yield y)
{
  int retDir;
  cached_local = 4;
  std::string localCache = g_conf()->rgw_local_cache_address;
  retDir = source->driver->get_block_dir_cpp()->get(block, y);

  //the object is cached some where; local or remote.
  if (retDir == 0){
    if (block->hostsList.size() > 0){
      for (auto &it : block->hostsList){
	if (it == localCache){
	  if (block->in_lsvd == false){
      	    cached_local = 1; //local cache
            return 0;
	  }
	  else{
	    if (g_conf()->rgw_d4n_lsvd_cache_enabled == true){ //if we have a LSVD cache
      	        cached_local = 2; //local_lsvd
	    }
	    else{
	      cached_local = 1;
	    }
            return 0;
	  }
	}
      }
      if (block->size < g_conf()->rgw_d4n_small_object_threshold){
	if (block->in_lsvd == true){
	  if (g_conf()->rgw_d4n_lsvd_use_enabled == true){ //if we have a LSVD server somewhere
      	    cached_local = 3; //remote_lsvd
	    //TODO: when we have more lsvd servers, we should hash the name and based on it
	    // find the lsvd cache address.
	  }
	}
	else{
	  cached_local = 4;
	}
	return 0;
      }

      //find the best remote cache to read from (network, cache usage, ...)
      //TODO: we should insert ILP algorithm here
      if (cached_local == 4){ //remote big object
	return 0;
      }
    }
  }
  else{
    cached_local = 0; //backend
    return 0;
  }
}

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;
  std::string version = source->get_object_version();
  std::string prefix;

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "cached_local: " << std::to_string(cached_local) << " ofs is: " << ofs << dendl;
  if (cached_local == 0){
    std::string bucketName = source->get_bucket()->get_name();
    std::string objName = source->get_key().get_oid();

    if (version.empty()) {
      version = source->get_instance();
    }

    if (version.empty()) { //for versioned objects, get_oid() returns an oid with versionId added
      prefix = source->get_bucket()->get_name() + "_" + source->get_key().get_oid();
    } else {
      prefix = source->get_bucket()->get_name() + "_" + version + "_" + source->get_key().get_oid();
    }
    source->set_prefix(prefix);
  }
  else{
    prefix = source->get_prefix();
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << " for ofs: " << ofs << " prefix: " << prefix << dendl;

  this->client_cb = cb;
  this->cb->set_client_cb(cb, dpp, &y);

  /* This algorithm stores chunks for ranged requests also in the cache, which might be smaller than obj_max_req_size
     One simplification could be to overwrite the smaller chunks with a bigger chunk of obj_max_req_size, and to serve requests for smaller
     chunks using the larger chunk, but all corner cases need to be considered like the last chunk which might be smaller than obj_max_req_size
     and also ranged requests where a smaller chunk is overwritten by a larger chunk size != obj_max_req_size */

  uint64_t obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;
  uint64_t start_part_num = 0;
  uint64_t part_num = ofs/obj_max_req_size; //part num of ofs wrt start of the object
  uint64_t adjusted_start_ofs = part_num*obj_max_req_size; //in case of ranged request, adjust the start offset to the beginning of a chunk/ part
  uint64_t diff_ofs = ofs - adjusted_start_ofs; //difference between actual offset and adjusted offset
  off_t len = (end - adjusted_start_ofs) + 1;
  uint64_t num_parts = (len%obj_max_req_size) == 0 ? len/obj_max_req_size : (len/obj_max_req_size) + 1; //calculate num parts based on adjusted offset
  //len_to_read is the actual length read from a part/ chunk in cache, while part_len is the length of the chunk/ part in cache 
  uint64_t cost = 0, len_to_read = 0, part_len = 0;

  aio = rgw::make_throttle(window_size, y);

  //this->offset = ofs;
  this->offset = adjusted_start_ofs;

  bool dirty = source->get_object_dirty();
    
  if (start_part_num == 0)
    this->set_first_block(true);

  do {
    int ret = 0;
    uint64_t id = adjusted_start_ofs, read_ofs = 0; //read_ofs is the actual offset to start reading from the current part/ chunk
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " start_part_num is: " << start_part_num <<  dendl;
    if (start_part_num == (num_parts - 1)) {
      len_to_read = len;
      part_len = len;
      cost = len;
    } else if (start_part_num < (num_parts - 1)) {
      len_to_read = obj_max_req_size;
      cost = obj_max_req_size;
      part_len = obj_max_req_size;
    }
    if (start_part_num > (num_parts - 1)){
      return 0;
      /*
      ret = drain(dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << ret << dendl;
	return ret;
      }*/
    }

    std::string oid_in_cache = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(part_len);

    std::string key = oid_in_cache;

    rgw::d4n::CacheBlockCpp block;
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_name();
    block.blockID = adjusted_start_ofs;
    block.size = part_len;

    if (dirty == true) 
      key = "D_" + oid_in_cache; //we keep track of dirty data in the cache for the metadata failure case

    this->blocks_info.insert(std::make_pair(id, std::make_pair(adjusted_start_ofs, part_len)));

    this->set_read_ofs(diff_ofs);

    this->cb->set_ofs(adjusted_start_ofs);

    ret = findLocation(dpp, &block, y);
    if (ret < 0){
      break;
    }

    std::string cacheLocation;
    ceph::bufferlist bl;

    if (cached_local == 1){ //local cache
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " for ofs: " << ofs << " Local Cache" <<  dendl;
      auto completed = source->driver->get_cache_driver()->get_async(dpp, y, aio.get(), key, read_ofs, len_to_read, cost, id);
      ret = flush(dpp, std::move(completed), y);
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " start_part_num is: " << start_part_num <<  dendl;
      if (ret < 0) {
	if (first_block == true){
          if (start_part_num == 0){
            ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " first block is true" << dendl;
	    this->last_part_done = true; //prevent infinte loop
            last_adjusted_ofs = adjusted_start_ofs;
	  }
          else
            last_adjusted_ofs = adjusted_start_ofs - obj_max_req_size;
	}
	else{
          if (start_part_num == 0)
            last_adjusted_ofs = adjusted_start_ofs;
          else
            last_adjusted_ofs = adjusted_start_ofs - obj_max_req_size;
        }
        drain(dpp, y);
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << ret << dendl;
        return ret;
      }
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " start_part_num is: " << start_part_num <<  dendl;
    }
    else if (cached_local == 2){ //local lsvd
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " for ofs: " << ofs << " Local LSVD" <<  dendl;
      auto completed = source->driver->get_lsvd_cache_driver()->get_async(dpp, y, aio.get(), prefix, ofs, len, 0, 0); 

      ldpp_dout(dpp, 20) << "D4NFilterObject::iterateLSVD:: " << __func__ << "(): Info: flushing data for oid: " << prefix << dendl;
      ret = lsvdFlush(dpp, std::move(completed), y);

      if (ret < 0) {
        lsvdDrain(dpp, y);
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, r= " << ret << dendl;
        return ret;
      }
      return lsvdDrain(dpp, y);
    }
    else if (cached_local == 3){ //remote lsvd
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " for ofs: " << ofs << " Remote LSVD" <<  dendl;
      cacheLocation = g_conf()->rgw_d4n_lsvd_cache_address;
      ret = getRemote(dpp, (long long)adjusted_start_ofs, (long long)adjusted_start_ofs+(long long)len_to_read-1, source->get_key().get_oid(), cacheLocation, &bl, y); //send it to the remote cache
      if (ret < 0){
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to read from Remote LSVD, r= " << ret << dendl;
	return ret;
      }
      remoteFlush(dpp, bl, source->get_creationTime(), y); //TODO
    }
    else if (cached_local == 4){ //remote cache
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " for ofs: " << ofs << " Remote Cache" <<  dendl;
      cacheLocation = block.hostsList.back(); //we read the object from the last cache accessing it
      ret = getRemote(dpp, (long long)adjusted_start_ofs, (long long)adjusted_start_ofs+(long long)len_to_read-1, source->get_key().get_oid(), cacheLocation, &bl, y); //send it to the remote cache
      if (ret < 0){
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to read from Remote Cache, r= " << ret << dendl;
	return ret;
      }
      remoteFlush(dpp, bl, source->get_creationTime(), y); 
    }
    else{ //backend
      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Reading data for oid: " << oid_in_cache << " from BACKEND!" << dendl;
      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
      
      ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " adjusted_start_ofs: " << adjusted_start_ofs << dendl;
      if (first_block == true){
        if (start_part_num == 0){
          ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " first block is true" << dendl;
	  this->last_part_done = true; //prevent infinte loop
          last_adjusted_ofs = adjusted_start_ofs;
	}
        else
          last_adjusted_ofs = adjusted_start_ofs - obj_max_req_size;
      }
      else{
        if (start_part_num == 0)
          last_adjusted_ofs = adjusted_start_ofs;
        else
          last_adjusted_ofs = adjusted_start_ofs - obj_max_req_size;
      }

      ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " last_adjusted_ofs:" << last_adjusted_ofs << dendl;
      ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " offset:" << offset << dendl;

      if (this->offset == last_adjusted_ofs)
	break;
      ret = drain(dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << ret << dendl;
	return ret;
      }

      ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " offset:" << offset << dendl;
      break;
      /*
      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << " for ofs: " << ofs << " adjusted_start_ofs: " << adjusted_start_ofs << dendl;
      //ret = next->iterate(dpp, adjusted_start_ofs, len_to_read, this->cb.get(), y);
      ret;
      }
      remoteFlush(dpp, bl, source->get_creationTime(), y);
    }
et = next->iterate(dpp, adjusted_start_ofs, len_to_read, client_cb, y);
      if (ret < 0){
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to read from backend, ofs is: " << ofs << " adjusted_start_ofs: " << adjusted_start_ofs << " r= " << ret << dendl;
	return ret;
      }*/
       /*
      if (start_part_num == (num_parts - 1)) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << " for ofs: " << ofs << dendl;
        return this->cb->flush_last_part();
      }
      */
    }
    
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " start_part_num is: " << start_part_num <<  dendl;
    if (start_part_num == (num_parts - 1)) {
      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << " for ofs: " << ofs << dendl;
      //return this->cb->flush_last_part();
      //this->cb->set_last_part(true);

      last_adjusted_ofs = adjusted_start_ofs;
      //last_adjusted_ofs = adjusted_start_ofs >= obj_max_req_size ? adjusted_start_ofs-obj_max_req_size : 0;
      ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " last_adjusted_ofs:" << last_adjusted_ofs << dendl;
      ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " offset:" << offset << dendl;
      //if (this->offset == last_adjusted_ofs)
      //break;
      return drain(dpp, y);
    } 
    else {
      adjusted_start_ofs += obj_max_req_size;
    }

    start_part_num += 1;
    len -= obj_max_req_size;
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " <<  __LINE__ << " start_part_num is: " << start_part_num <<  dendl;
  } while (start_part_num < num_parts);

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << " for ofs: " << ofs << " adjusted_start_ofs: " << adjusted_start_ofs << dendl;
  
  ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " last_adjusted_ofs:" << last_adjusted_ofs << dendl;
  ldpp_dout(dpp, 20) << __func__ << " " << __LINE__ << " offset:" << offset << dendl;

  /*
  if (start_part_num != 0) {
    ofs = adjusted_start_ofs;
  }
  */
  
  this->cb->set_adjusted_start_ofs(adjusted_start_ofs);
  this->cb->set_read_ofs(diff_ofs);
  /*
  if (start_part_num == 0) {
    this->cb->set_first_block(true);
  }
  */

  this->cb->set_ofs(adjusted_start_ofs);

  
  auto ret = next->iterate(dpp, adjusted_start_ofs, end, this->cb.get(), y);  
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to fetch object from backend store, r= " << ret << dendl;
    return ret;
  }
    
  return this->cb->flush_last_part();
  
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::flush_last_part()
{
  last_part = true;
  return handle_data(bl_rem, 0, bl_rem.length());
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  ldpp_dout(dpp, 20) << "AMIN:DEBUG "<< __func__ << ": bl_ofs is: " << bl_ofs << dendl;
  auto rgw_get_obj_max_req_size = g_conf()->rgw_get_obj_max_req_size;

  if (!last_part && bl.length() <= rgw_get_obj_max_req_size) {
    if (first_block == true){
      //auto r = client_cb->handle_data(bl, bl_ofs+read_ofs, bl_len-read_ofs); //AMIN-RANGE
      auto r = client_cb->handle_data(bl, bl_ofs+read_ofs, bl_len);
      this->set_first_block(false);
      if (r < 0) {
        return r;
      }
    }
    else{
      auto r = client_cb->handle_data(bl, bl_ofs, bl_len);
      if (r < 0) {
        return r;
      }
    }
      

  }

  //Accumulating data from backend store into rgw_get_obj_max_req_size sized chunks and then writing to cache
  if (write_to_cache) {
    //rgw::d4n::CacheBlock block, existing_block;
    rgw::d4n::CacheBlockCpp block;
    //rgw::d4n::CacheObj object, existing_object;
    rgw::d4n::CacheObjectCpp object;
    //rgw::d4n::BlockDirectory* blockDir = source->driver->get_block_dir();
    rgw::d4n::RGWBlockDirectory* blockDir = source->driver->get_block_dir_cpp();
    //rgw::d4n::ObjectDirectory* objectDir = source->driver->get_obj_dir();
    rgw::d4n::RGWObjectDirectory* objectDir = source->driver->get_obj_dir_cpp();

    block.hostsList.push_back(blockDir->cct->_conf->rgw_local_cache_address); 
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_name();
    std::stringstream s;
    block.cacheObj.creationTime = std::to_string(ceph::real_clock::to_time_t(source->get_mtime())); //TODO
    block.cacheObj.dirty = false;

    object.hostsList.push_back(blockDir->cct->_conf->rgw_local_cache_address); 
    object.objName = source->get_key().get_oid();
    object.bucketName = source->get_bucket()->get_name();
    object.creationTime = std::to_string(ceph::real_clock::to_time_t(source->get_mtime())); //TODO
    object.dirty = false;
    object.size = source->get_obj_size(); 

    RGWAccessControlPolicy acl = source->get_acl();
    bufferlist bl_attr;
    acl.encode(bl_attr);
    rgw::sal::Attrs obj_attrs; 
    obj_attrs[RGW_ATTR_ACL] = std::move(bl_attr);
    object.attrs = obj_attrs;

    bool dirty = false;
    time_t creationTime = ceph::real_clock::to_time_t(source->get_mtime());

    //populating fields needed for building directory index
    Attrs attrs; // empty attrs for cache sets
    std::string version = source->get_object_version();
    std::string prefix = source->get_prefix();
    if (version.empty()) {
      version = source->get_instance();
    }
    object.version = version;

    ldpp_dout(dpp, 20) << __func__ << ": version stored in update method is: " << version << dendl;

    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      //std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len); //AMIN-RANGE
      std::string oid = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(bl_len);
      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
        //block.blockID = ofs; //AMIN-RANGE
        block.blockID = adjusted_start_ofs;
        block.size = bl.length();
        block.version = version;
        block.dirty = false; //Reading from the backend, data is clean
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
	  
          if (ret == 0) { 
  	    std::string objEtag = "";
 	    //filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, dirty, creationTime,  source->get_bucket()->get_owner(), *y); //AMIN-RANGE
 	    filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, adjusted_start_ofs, bl.length(), version, dirty, creationTime,  source->get_bucket()->get_owner(), *y);
	    filter->get_policy_driver()->get_cache_policy()->updateObj(dpp, prefix, version, dirty, source->get_obj_size(), creationTime, source->get_bucket()->get_owner(), objEtag, *y);
	   
    	    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
	    // Store block in directory
            if (blockDir->set(&block, *y) < 0) //should we revert previous steps if this step fails?
	      ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
    	    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
              if (objectDir->set(&object, *y) < 0) 
		ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): ObjectDirectory set method failed." << dendl;
          } else {
	          ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed with error: " << ret << dendl;
          }
        }
      }
    } else if (bl.length() == rgw_get_obj_max_req_size && bl_rem.length() == 0) { // if bl is the same size as rgw_get_obj_max_req_size, write it to cache
      //std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_len); //AMIN-RANGE
      std::string oid = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(bl_len);
      //block.blockID = ofs; //AMIN-RANGE
      block.blockID = adjusted_start_ofs;
      block.size = bl.length();
      block.version = version;
      block.dirty = dirty;

      if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) { //In case of concurrent reads for the same object, the block is already cached
        auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
        if (ret == 0) {
          ret = filter->get_cache_driver()->put(dpp, oid, bl, bl.length(), attrs, *y);
	 
          if (ret == 0) {
            //filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl.length(), version, dirty, creationTime, source->get_bucket()->get_owner(), *y);//AMIN_RANGE
            filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, adjusted_start_ofs, bl.length(), version, dirty, creationTime, source->get_bucket()->get_owner(), *y);
	    
    	    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
              if (blockDir->set(&block, *y) < 0)
		ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
    	    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
          } else {
            ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed with error: " << ret << dendl;
          } 
        }
      }
      //ofs += bl_len; //AMIN_RANGE
      adjusted_start_ofs += bl_len;
    } else { //copy data from incoming bl to bl_rem till it is rgw_get_obj_max_req_size, and then write it to cache
      uint64_t rem_space = rgw_get_obj_max_req_size - bl_rem.length();
      uint64_t len_to_copy = rem_space > bl.length() ? bl.length() : rem_space;
      bufferlist bl_copy;

      bl.splice(0, len_to_copy, &bl_copy);
      bl_rem.claim_append(bl_copy);

      if (bl_rem.length() == rgw_get_obj_max_req_size) {
        //std::string oid = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(bl_rem.length()); // AMIN_RANGE
        std::string oid = prefix + "_" + std::to_string(adjusted_start_ofs) + "_" + std::to_string(bl_rem.length());
          if (!filter->get_policy_driver()->get_cache_policy()->exist_key(oid)) {
          //block.blockID = ofs; // AMIN_RANGE
          block.blockID = adjusted_start_ofs;
          block.size = bl_rem.length();
          block.version = version;
	  block.dirty = dirty;

          auto ret = filter->get_policy_driver()->get_cache_policy()->eviction(dpp, block.size, *y);
          if (ret == 0) {
            ret = filter->get_cache_driver()->put(dpp, oid, bl_rem, bl_rem.length(), attrs, *y);
	    
            if (ret == 0) {
              //filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, ofs, bl_rem.length(), version, dirty, creationTime, source->get_bucket()->get_owner(), *y); //AMIN_RANGE
              filter->get_policy_driver()->get_cache_policy()->update(dpp, oid, adjusted_start_ofs, bl_rem.length(), version, dirty, creationTime, source->get_bucket()->get_owner(), *y);
	     
              // Store block in directory
    	      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
                if (blockDir->set(&block, *y) < 0)
                  ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            } else {
              ldpp_dout(dpp, 0) << "D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::" << __func__ << "(): put() to cache backend failed with error: " << ret << dendl;
            } 
          } else {
            ldpp_dout(dpp, 20) << "D4N Filter: " << __func__ << " An error occured during eviction: " << " error: " << ret << dendl;
          }
        }

        //ofs += bl_rem.length(); //AMIN_RANGE
        adjusted_start_ofs += bl_rem.length();

        bl_rem.clear();
        bl_rem = std::move(bl);
      }//bl_rem.length()
    }
  }

  /* Clean-up:
  1. do we need to clean up older versions of the cache backend, when we update version in block directory?
  2. do we need to clean up keys belonging to older versions (the last blocks), in case the size of newer version is different
  3. do we need to revert the cache ops, in case the directory ops fail
  */

  return 0;
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
                                                   optional_yield y, uint32_t flags)
{
  rgw::d4n::CacheObjectCpp object = rgw::d4n::CacheObjectCpp{ // TODO: Add logic to ObjectDirectory del method to also delete all blocks belonging to that object
			     .objName = source->get_key().get_oid(),
			     .bucketName = source->get_bucket()->get_name()
			   };

  if (source->driver->get_obj_dir_cpp()->del(&object, y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): ObjectDirectory del method failed." << dendl;

  Attrs::iterator attrs;
  Attrs currentattrs = source->get_attrs();
  std::vector<std::string> currentFields;
  
  /* Extract fields from current attrs */
  for (attrs = currentattrs.begin(); attrs != currentattrs.end(); ++attrs) {
    currentFields.push_back(attrs->first);
  }

  if (source->driver->get_cache_driver()->del(dpp, source->get_key().get_oid(), y) < 0) 
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): CacheDriver del method failed." << dendl;

  return next->delete_obj(dpp, y, flags);
}

int D4NFilterWriter::sendRemote(const DoutPrefixProvider* dpp, rgw::d4n::CacheObjectCpp *object, std::string remoteCacheAddress, std::string key, bufferlist* out_bl, optional_yield y)
{
  bufferlist in_bl;
  RGWRemoteD4NGetCB cb(&in_bl);
  std::string bucketName = object->bucketName;
 
  RGWAccessKey accessKey;
  std::string findKey;
  
  auto user = obj->get_bucket()->get_owner();
  std::unique_ptr<rgw::sal::User> c_user = driver->get_user(user);
  int ret = c_user->load_user(dpp, y);
  if (ret < 0) {
    return -EPERM;
  }

  if (c_user->get_info().access_keys.empty()) {
    return -EINVAL;
  }

  accessKey.id = c_user->get_info().access_keys.begin()->second.id;
  accessKey.key = c_user->get_info().access_keys.begin()->second.key;

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;                                                            

  auto sender = new RGWRESTStreamRWRequest(dpp->get_cct(), "PUT", remoteCacheAddress, &cb, NULL, NULL, "", host_style);

  ret = sender->send_request(dpp, accessKey, extra_headers, obj->get_obj(), nullptr);
  if (ret < 0) {                                                                                      
    delete sender;                                                                                       
    return ret;                                                                                       
  }                                                                                                   
  
  ret = sender->complete_request(y);                                                            
  if (ret < 0){
    delete sender;                                                                                   
    return ret;                                                                                   
  }

  return 0;
}	



int D4NFilterWriter::prepare(optional_yield y) 
{
  this->objVersion = obj->get_instance();
  startTime = time(NULL);
  if (driver->get_cache_driver()->delete_data(save_dpp, obj->get_key().get_oid(), y) < 0) 
    ldpp_dout(save_dpp, 10) << "D4NFilterWriter::" << __func__ << "(): CacheDriver delete_data method failed." << dendl;
  d4n_writecache = g_conf()->d4n_writecache_enabled;
  lsvd_cache_enabled = g_conf()->rgw_d4n_lsvd_cache_enabled;
  lsvd_cache_used = g_conf()->rgw_d4n_lsvd_use_enabled;
  lsvd_counter = 0;
  if (d4n_writecache == false){ //lsvd cache is also a part of write procedure
    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): calling next iterate" << dendl;
    return next->prepare(y);
  }
  else
    return 0;
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{
    lsvd_counter += 1;
    bufferlist bl = data;
    off_t bl_len = bl.length();
    off_t ofs = offset;
    bool dirty = true;
    rgw::d4n::CacheBlockCpp block, existing_block;
    auto creationTime = startTime;
    bool lsvd_used = false;

    std::string prefix;
    if (objVersion.empty()) { //for versioned objects, get_oid() returns an oid with versionId added
      prefix =obj->get_bucket()->get_name() + "_" + obj->get_key().get_oid();
    } else {
      prefix = obj->get_bucket()->get_name() + "_" + objVersion + "_" + obj->get_key().get_oid();
    }

    ldpp_dout(save_dpp, 20) << "AMIN: " << __func__ << "(): prefix: "  << prefix << dendl;
    rgw::d4n::RGWBlockDirectory* blockDir = driver->get_block_dir_cpp();
    rgw::d4n::RGWObjectDirectory* objectDir = driver->get_obj_dir_cpp();

    block.cacheObj.bucketName = obj->get_bucket()->get_name();
    block.cacheObj.objName = obj->get_key().get_oid();
    block.cacheObj.dirty = dirty;
    block.version = objVersion;
    existing_block.cacheObj.objName = block.cacheObj.objName;
    existing_block.cacheObj.bucketName = block.cacheObj.bucketName;


    int ret = 0;

    if (d4n_writecache == false){
//      ldpp_dout(save_dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
      ret = next->process(std::move(data), offset);
      if (ret < 0){
          ldpp_dout(save_dpp, 1) << "D4NFilterObject::D4NFilterWriteOp::process" << __func__ << "(): ERROR: writting data to the backend failed!" << dendl;
	  return ret;
      }
    }
    else if (lsvd_cache_enabled){//local lsvd cache
//      ldpp_dout(save_dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
      if (lsvd_counter == 1 && bl.length() < g_conf()->rgw_d4n_small_object_threshold){ //small object
	lsvd_used = true;
//      ldpp_dout(save_dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
    	rgw::d4n::CacheObjectCpp object, existing_object;
        object.bucketName = obj->get_bucket()->get_name();
        object.objName = obj->get_key().get_oid();
        std::string oid = prefix;
        object.size = bl.length();
	object.in_lsvd = true;
	object.version = objVersion;
	object.creationTime = std::to_string(creationTime);
        object.hostsList.push_back(blockDir->cct->_conf->rgw_local_cache_address);

        RGWAccessControlPolicy acl = obj->get_acl();
        bufferlist bl_attr;
        acl.encode(bl_attr);
        rgw::sal::Attrs obj_attrs; 
        obj_attrs[RGW_ATTR_ACL] = std::move(bl_attr);
        object.attrs = obj_attrs;

	if (bl.length() > 0) {          
	  //TODO : AMIN uncomment this
          ret = driver->get_lsvd_cache_driver()->put(save_dpp, prefix, bl, bl.length(), obj->get_attrs(), y);
          if (ret == 0) {
            //if (!objectDir->exist_key(&object, y)) {
              if (objectDir->set(&object, y) < 0)
  	        ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory set method failed." << dendl;
            /*} else {
              existing_object.bucketName = obj->get_bucket()->get_name();
              existing_object.objName = obj->get_key().get_oid();
              if (objectDir->get(&existing_object, y) < 0) {
                ldpp_dout(save_dpp, 10) << "Failed to fetch existing object for: " << existing_object.objName << dendl;
              } else {
                if (existing_object.version != object.version) {
                  if (objectDir->del(&existing_object, y) < 0)
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory del method failed." << dendl;
                  if (objectDir->set(&object, y) < 0) //new versioned block will have new version, hostsList etc, how about globalWeight?
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory set method failed." << dendl;
                } else {
                  if (objectDir->update_field(&object, "objHosts", blockDir->cct->_conf->rgw_local_cache_address, y) < 0)
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory update_field method failed for hostsList." << dendl;
                }
              }
	    }*/
          } else {
            ldpp_dout(save_dpp, 1) << "D4NFilterObject::D4NFilterWriteOp::process" << __func__ << "(): ERROR: writting data to the local LSVD cache failed!" << dendl;
	    return ret;
	  }
	}
      }
    }
    else if (lsvd_cache_used){//remote lsvd
//      ldpp_dout(save_dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
      if (lsvd_counter == 1 && bl.length() < g_conf()->rgw_d4n_small_object_threshold){ //small object
	lsvd_used = true;
//      ldpp_dout(save_dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
    	rgw::d4n::CacheObjectCpp object, existing_object;
        object.bucketName = obj->get_bucket()->get_name();
        object.objName = obj->get_key().get_oid();
        std::string oid = prefix;
        object.size = bl.length();
	object.in_lsvd = true;
	object.version = objVersion;
	object.creationTime = std::to_string(creationTime);
	bufferlist out_bl;
        object.hostsList.push_back(blockDir->cct->_conf->rgw_local_cache_address);

        RGWAccessControlPolicy acl = obj->get_acl();
        bufferlist bl_attr;
        acl.encode(bl_attr);
        rgw::sal::Attrs obj_attrs; 
        obj_attrs[RGW_ATTR_ACL] = std::move(bl_attr);
        object.attrs = obj_attrs;

	if (bl.length() > 0) {          
	  ret = sendRemote(save_dpp, &object, blockDir->cct->_conf->rgw_d4n_lsvd_cache_address, oid, &out_bl, y);
          if (ret == 0) {
            //if (!objectDir->exist_key(&object, y)) {
              if (objectDir->set(&object, y) < 0)
  	        ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory set method failed." << dendl;
            /*} else {
              existing_object.bucketName = obj->get_bucket()->get_name();
              existing_object.objName = obj->get_key().get_oid();
              if (objectDir->get(&existing_object, y) < 0) {
                ldpp_dout(save_dpp, 10) << "Failed to fetch existing object for: " << existing_object.objName << dendl;
              } else {
                if (existing_object.version != object.version) {
                  if (objectDir->del(&existing_object, y) < 0)
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory del method failed." << dendl;
                  if (objectDir->set(&object, y) < 0) //new versioned block will have new version, hostsList etc, how about globalWeight?
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory set method failed." << dendl;
                } else {
                  if (objectDir->update_field(&object, "objHosts", blockDir->cct->_conf->rgw_d4n_lsvd_cache_address, y) < 0)
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): objectDirectory update_field method failed for hostsList." << dendl;
                }
              }
	    }*/
          } else {
            ldpp_dout(save_dpp, 1) << "D4NFilterObject::D4NFilterWriteOp::process" << __func__ << "(): ERROR: writting data to the remote LSVD cache failed!" << dendl;
	    return ret;
	  }
	}
      }
    }
    if (lsvd_used == false && d4n_writecache == true){
//      ldpp_dout(save_dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
      std::string oid = prefix + "_" + std::to_string(ofs);
      std::string key = "D_" + oid + "_" + std::to_string(bl_len);
      std::string oid_in_cache = oid + "_" + std::to_string(bl_len);
      block.size = bl.length();
      block.blockID = ofs;
      block.dirty = true;
      block.hostsList.push_back(blockDir->cct->_conf->rgw_local_cache_address);
      dirty = true;
      ret = driver->get_policy_driver()->get_cache_policy()->eviction(save_dpp, block.size, y);
      if (ret == 0) {
        //Should we replace each put_async with put, to ensure data is actually written to the cache before updating the data structures and before the lock is released?
	if (bl.length() > 0) {          
          ret = driver->get_cache_driver()->put(save_dpp, key, bl, bl.length(), obj->get_attrs(), y);
          if (ret == 0) {
 	    driver->get_policy_driver()->get_cache_policy()->update(save_dpp, oid_in_cache, ofs, bl.length(), objVersion, dirty, creationTime,  obj->get_bucket()->get_owner(), y);
            //if (!blockDir->exist_key(&block, y)) {
              if (blockDir->set(&block, y) < 0) //should we revert previous steps if this step fails?
  	        ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory set method failed." << dendl;
            /*} else {
              existing_block.blockID = block.blockID;
              existing_block.size = block.size;
              if (blockDir->get(&existing_block, y) < 0) {
                ldpp_dout(save_dpp, 10) << "Failed to fetch existing block for: " << existing_block.cacheObj.objName << " blockID: " << existing_block.blockID << " block size: " << existing_block.size << dendl;
              } else {
                if (existing_block.version != block.version) {
                  if (blockDir->del(&existing_block, y) < 0) //delete existing block
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory del method failed." << dendl;
                  if (blockDir->set(&block, y) < 0) //new versioned block will have new version, hostsList etc, how about globalWeight?
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory set method failed." << dendl;
                } else {
                  if (blockDir->update_field(&block, "blockHosts", blockDir->cct->_conf->rgw_local_cache_address, y) < 0)
                    ldpp_dout(save_dpp, 10) << "D4NFilterObject::D4NFilterWriteOp::" << __func__ << "(): BlockDirectory update_field method failed for hostsList." << dendl;
                }
              }
	    }*/
          } else {
            ldpp_dout(save_dpp, 1) << "D4NFilterObject::D4NFilterWriteOp::process" << __func__ << "(): ERROR: writting data to the cache failed!" << dendl;
	    return ret;
	  }
	}
      }
    } 
    return 0;
}

int D4NFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  auto creationTime = startTime;
  if (d4n_writecache == true){
    bool dirty = true;
    std::vector<std::string> hostsList = {};
    std::string objEtag = etag;

    std::string prefix;
    if (objVersion.empty()) { //for versioned objects, get_oid() returns an oid with versionId added
      prefix = obj->get_bucket()->get_name() + "_" + obj->get_key().get_oid();
    } else {
      prefix = obj->get_bucket()->get_name() + "_" + objVersion + "_" + obj->get_key().get_oid();
    }

    RGWAccessControlPolicy acl = obj->get_acl();
    bufferlist bl_attr;
    acl.encode(bl_attr);
    rgw::sal::Attrs obj_attrs; 
    obj_attrs[RGW_ATTR_ACL] = std::move(bl_attr);

    ldpp_dout(save_dpp, 10) << "Amin: D4NFilterWriter::" << __func__ << "()" << dendl;

    hostsList = { driver->get_block_dir_cpp()->cct->_conf->rgw_local_cache_address };
    if ((lsvd_cache_enabled == false && lsvd_cache_used == false) || (accounted_size >= g_conf()->rgw_d4n_small_object_threshold)) { 
      rgw::d4n::CacheObjectCpp object = rgw::d4n::CacheObjectCpp{
		 .objName = obj->get_key().get_oid(), 
		 .bucketName = obj->get_bucket()->get_name(),
		 .creationTime = std::to_string(creationTime), 
		 .dirty = dirty,
		 .hostsList = hostsList,
		 .version = objVersion,
		 .size = accounted_size,
		 .attrs = obj_attrs
              };

      //TODO: check if the key exist  in the directory, if yes, uopdate it instead of set
      if (driver->get_obj_dir_cpp()->set(&object, y) < 0) 
        ldpp_dout(save_dpp, 10) << "D4NFilterWriter::" << __func__ << "(): ObjectDirectory set method failed." << dendl;

      driver->get_policy_driver()->get_cache_policy()->updateObj(save_dpp, prefix, objVersion, dirty, accounted_size, creationTime, obj->get_bucket()->get_owner(), objEtag, y);
      return 0;
    }
    else
      return 0;
  }
  /* Retrieve complete set of attrs */
  int ret = next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, rctx, flags);
  obj->get_obj_attrs(rctx.y, save_dpp, NULL);

  /* Append additional metadata to attributes */ 
  rgw::sal::Attrs baseAttrs = obj->get_attrs();
  rgw::sal::Attrs attrs_temp = baseAttrs;
  buffer::list bl;
  RGWObjState* astate;
  obj->get_obj_state(save_dpp, &astate, rctx.y);

  bl.append(std::to_string(creationTime));
  baseAttrs.insert({"mtime", bl});
  bl.clear();

  bl.append(std::to_string(obj->get_obj_size()));
  baseAttrs.insert({"object_size", bl});
  bl.clear();

  bl.append(std::to_string(accounted_size));
  baseAttrs.insert({"accounted_size", bl});
  bl.clear();
 
  bl.append(std::to_string(astate->epoch));
  baseAttrs.insert({"epoch", bl});
  bl.clear();

  if (obj->have_instance()) {
    bl.append(obj->get_instance());
    baseAttrs.insert({"version_id", bl});
    bl.clear();
  } else {
    bl.append(""); /* Empty value */
    baseAttrs.insert({"version_id", bl});
    bl.clear();
  }

  auto iter = attrs_temp.find(RGW_ATTR_SOURCE_ZONE);
  if (iter != attrs_temp.end()) {
    bl.append(std::to_string(astate->zone_short_id));
    baseAttrs.insert({"source_zone_short_id", bl});
    bl.clear();
  } else {
    bl.append("0"); /* Initialized to zero */
    baseAttrs.insert({"source_zone_short_id", bl});
    bl.clear();
  }

  baseAttrs.insert(attrs.begin(), attrs.end());
  
  //bufferlist bl_empty;
  //int putReturn = driver->get_cache_driver()->
  //	  put(save_dpp, obj->get_key().get_oid(), bl_empty, accounted_size, baseAttrs, y); /* Data already written during process call */
  /*
  if (putReturn < 0) {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache put operation failed." << dendl;
  } else {
    ldpp_dout(save_dpp, 20) << "D4N Filter: Cache put operation succeeded." << dendl;
  }
  */
  return ret;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, void* io_context)
{
  rgw::sal::D4NFilterDriver* driver = new rgw::sal::D4NFilterDriver(next, *static_cast<boost::asio::io_context*>(io_context));

  return driver;
}

}
