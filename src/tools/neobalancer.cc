#include <iostream>

#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "include/rados/librados.hpp"
#include "osd/OSDMap.h"
#include "mon/PGMap.h"
#include "global/global_init.h"

void print_projected_df(const OSDMap &osd_map, const PGMap &pg_map, int pool_id) {
  const pg_pool_t* pool =  osd_map.get_pg_pool(pool_id);
  if (!pool) {
    return;
  }
  int pg_num = pool->get_pg_num();
  int pool_size = pool->get_size();
  int num_pg_slots = pg_num * pool_size;
  
  float raw_used_rate = osd_map.pool_raw_used_rate(pool_id);
  map<int,set<pg_t>> pgs_by_osd;
  for (int ps = 0; ps < pg_num; ++ps) {
    pg_t pg(ps, pool_id);
    vector<int> up;
    osd_map.pg_to_up_acting_osds(pg, &up, nullptr, nullptr, nullptr);
    for (auto osd : up) {
      if (osd != CRUSH_ITEM_NONE) {
        pgs_by_osd[osd].insert(pg);
      }
    }
  }
  
  float full_ratio = osd_map.get_full_ratio();
  int64_t min_free_bytes = std::numeric_limits<int64_t>::max();
  int bottleneck_osd = -1;
  for (const auto &[osd_id, pgs] : pgs_by_osd) {
    float osd_pg_slots = (float) pgs.size();
    float osd_weight = osd_pg_slots / (float) num_pg_slots; 
    int64_t osd_total_bytes = 0;
    for (const pg_t &pg : pgs) {
      int64_t num_bytes = pg_map.pg_stat.at(pg).stats.sum.num_bytes;
      osd_total_bytes += num_bytes;
    }
    const auto &statfs = pg_map.osd_stat.at(osd_id).statfs;
    int64_t usable_bytes = statfs.total * full_ratio; 
    const float eff = ((float) statfs.data_stored) / ((float) statfs.allocated);
    int64_t free_bytes = (usable_bytes - osd_total_bytes)  * eff / (osd_weight * raw_used_rate); 
    if (free_bytes < min_free_bytes) {
      min_free_bytes = free_bytes;
      bottleneck_osd = osd_id;
    }
  }
  std::cout << "projected cluster free space: " << min_free_bytes <<
    " (bytes) bottleneck osd: " << bottleneck_osd << std::endl;
}

void print_inc_upmaps(const OSDMap::Incremental& pending_inc, int fd)
{
  ostringstream ss;
  for (auto& i : pending_inc.old_pg_upmap) {
    ss << "ceph osd rm-pg-upmap " << i << std::endl;
  }
  for (auto& i : pending_inc.new_pg_upmap) {
    ss << "ceph osd pg-upmap " << i.first;
    for (auto osd : i.second) {
      ss << " " << osd;
    }
    ss << std::endl;
  }
  for (auto& i : pending_inc.old_pg_upmap_items) {
    ss << "ceph osd rm-pg-upmap-items " << i << std::endl;
  }
  for (auto& i : pending_inc.new_pg_upmap_items) {
    ss << "ceph osd pg-upmap-items " << i.first;
    for (auto p : i.second) {
      ss << " " << p.first << " " << p.second;
    }
    ss << std::endl;
  }
  string s = ss.str();
  int r = safe_write(fd, s.c_str(), s.size());
  if (r < 0) {
    cerr << "error writing output: " << cpp_strerror(r) << std::endl;
    exit(1);
  }
}

int main(int argc, const char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    return EXIT_FAILURE;
  }
  
  librados::Rados cluster;
  char cluster_name[] = "ceph";
  char user_name[] = "client.admin";
  uint64_t flags = 0;
  
  int pool_id = -1;

  std::ostringstream err;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &pool_id, err, "--pool", (char*)NULL)) {
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        exit(EXIT_FAILURE);
      }
    } else {
      ++i;
    } 
  }
    
  if (pool_id < 0) {
    std::cerr << argv[0] << ": must specify the pool id with --pool" << std::endl;    
    return EXIT_FAILURE;
  }
  
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  
  int ret = 0;

  ret = cluster.init2(user_name, cluster_name, flags);
  if (ret < 0) {
    std::cerr << "Couldn't initialize the cluster handle! error " << ret << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Created a cluster handle." << std::endl;
  }

  ret = cluster.conf_read_file(NULL);
  if (ret < 0) {
    std::cerr << "failed to read ceph config file. error: " << ret << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Read cluster configuration file." << std::endl;
  }
  
  ret = cluster.connect();
  if (ret < 0) {
    std::cerr << "Couldn't connect to cluster! error " << ret << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Connected to the cluster." << std::endl;
  }
  
  PGMap pgmap;
  OSDMap osdmap;
  bufferlist outbl;

  ret = cluster.mgr_command("{\"prefix\": \"pg getmap\"}", {}, &outbl, NULL);
  if (ret < 0) {
    std::cerr << "Failed to get pgmap! error " << ret << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Got PG map." << std::endl;
  }
  
  decode(pgmap, outbl);
  std::cout << "PG map version: " << pgmap.version << std::endl;
  
  outbl.clear();

  ret = cluster.mon_command("{\"prefix\": \"osd getmap\"}", {}, &outbl, NULL);
  if (ret < 0) {
    std::cerr << "Failed to get osdmap! error " << ret << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Got OSD map." << std::endl;
  }
  
  decode(osdmap, outbl);
  std::cout << "OSD map fsid: " << osdmap.get_fsid() << std::endl;
  
  OSDMap::Incremental pending_inc(osdmap.get_epoch()+1);
  pending_inc.fsid = osdmap.get_fsid();
  std::cout << "before df: " << std::endl;
  print_projected_df(osdmap, pgmap, pool_id);
  ret = osdmap.calc_pg_upmaps_max_avail_storage(g_ceph_context, pgmap, pool_id, &pending_inc);
  if (ret < 0) {
    std::cerr << "Failed to calc pg upmaps! error " << ret << std::endl;
    return EXIT_FAILURE;
  } else {
    if (ret > 0) {
      print_inc_upmaps(pending_inc, STDOUT_FILENO);
      ret = osdmap.apply_incremental(pending_inc);
      if (ret < 0) {
        std::cerr << "Failed to apply incremental! error " << ret << std::endl;
       return EXIT_FAILURE;
      }
    } else {
      std::cout << "Nothing to do." << std::endl;
    }
    std::cout << "Finished calc pg upmaps." << std::endl;
  }
  std::cout << "after df: " << std::endl;
  print_projected_df(osdmap, pgmap, pool_id);
  return 0;
}
