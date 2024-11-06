//
// Created by root on 10/17/24.
//

#ifndef ECEXTENTCACHE_H
#define ECEXTENTCACHE_H

#include "ECUtil.h"

namespace ECExtentCache {

  class Address;
  class Line;
  class PG;
  class LRU;
  class Object;
  class Op;
  typedef std::shared_ptr<Op> OpRef;

  struct BackendRead {
    virtual void backend_read(hobject_t oid, std::map<int, extent_set> const &request) = 0;
    virtual ~BackendRead() = default;
  };

  class PG
  {
    friend class Object;

    std::map<hobject_t, Object> objects;
    BackendRead &backend_read;
    LRU &lru;
    const ECUtil::stripe_info_t &sinfo;
    std::list<OpRef> waiting_ops;
    void cache_maybe_ready();

    OpRef request(GenContextURef<OpRef &> &&ctx, hobject_t const &oid, std::optional<std::map<int, extent_set>> const &to_read, std::map<int, extent_set> const &write, uint64_t projected_size);

  public:
    explicit PG(BackendRead &backend_read,
      LRU &lru, const ECUtil::stripe_info_t &sinfo) :
      backend_read(backend_read),
      lru(lru),
      sinfo(sinfo) {}

    // Insert some data into the cache.
    void read_done(hobject_t const& oid, ECUtil::shard_extent_map_t const&& update);
    void write_done(OpRef &op, ECUtil::shard_extent_map_t const&& update);
    void complete(OpRef &read);
    void discard_lru();
    bool contains_object(hobject_t const &oid);
    uint64_t get_projected_size(hobject_t const &oid);

    template<typename CacheReadyCb>
    OpRef request(hobject_t const &oid,
      std::optional<std::map<int, extent_set>> const &to_read,
      std::map<int, extent_set> const &write,
      uint64_t projected_size,
      CacheReadyCb &&ready_cb) {

      GenContextURef<OpRef &> ctx = make_gen_lambda_context<OpRef &, CacheReadyCb>(
            std::forward<CacheReadyCb>(ready_cb));

      return request(std::move(ctx), oid, to_read, write, projected_size);
    }
    bool idle(hobject_t &oid) const;
  };

  class LRU {
    friend class PG;
    friend class Object;

    std::list<Line> lru;
    uint64_t max_size = 0;
    uint64_t size = 0;
    ceph::mutex mutex = ceph::make_mutex("ECExtentCache::LRU");;

    void free_maybe();
    void free_to_size(uint64_t target_size);
    void discard();
    void pin(OpRef &op, uint64_t alignment, Object &object);
    std::map<hobject_t, std::list<uint64_t>> unpin(OpRef &op, uint64_t alignment);
    void inc_size(uint64_t size);
    void dec_size(uint64_t size);
  public:
    explicit LRU(uint64_t max_size) : max_size(max_size) {}
  };


  class Op
  {
    friend class Object;
    friend class PG;
    friend class LRU;

    Object &object;
    std::optional<std::map<int, extent_set>> reads;
    std::map<int, extent_set> writes;
    std::optional<ECUtil::shard_extent_map_t> result;
    bool complete = false;
    GenContextURef<OpRef &> cache_ready_cb;

    extent_set get_pin_eset(uint64_t alignment);

  public:
    explicit Op(GenContextURef<OpRef &> &&cache_ready_cb, Object &object) :
      object(object), cache_ready_cb(std::move(cache_ready_cb)) {}
    std::optional<ECUtil::shard_extent_map_t> get_result() { return result; }
    std::map<int, extent_set> get_writes() { return writes; }

  };

  class Object
  {

    friend class PG;
    friend class Op;
    friend class Line;
    friend class LRU;


    PG &pg;
    hobject_t oid;
    ECUtil::stripe_info_t const &sinfo;
    std::map<int, extent_set> requesting;
    std::map<int, extent_set> reading;
    std::map<int, extent_set> writing;
    ECUtil::shard_extent_map_t cache;
    unordered_map<uint64_t, Line> lines;
    int active_ios = 0;
    uint64_t projected_size = 0;

    void request(OpRef &op);
    void send_reads();
    uint64_t read_done(ECUtil::shard_extent_map_t const &result);
    uint64_t write_done(OpRef &op, ECUtil::shard_extent_map_t const &result);
    void check_buffers_pinned(ECUtil::shard_extent_map_t const &buffers);
    void check_cache_pinned();
    uint64_t insert(ECUtil::shard_extent_map_t const &buffers);
    void unpin(OpRef &op);
    void delete_maybe();
    uint64_t erase_line(Line &l);

  public:
    Object(PG &pg, hobject_t oid) : pg(pg), oid(oid), sinfo(pg.sinfo), cache(&pg.sinfo) {}
  };


  class Line
  {
  public:
    bool in_lru = false;
    int ref_count = 0;
    uint64_t offset;
    Object &object;

    Line(Object &object, uint64_t offset) : offset(offset) , object(object) {}

    friend bool operator==(const Line& lhs, const Line& rhs)
    {
      return lhs.in_lru == rhs.in_lru
        && lhs.ref_count == rhs.ref_count
        && lhs.offset == rhs.offset
        && lhs.object.oid == rhs.object.oid;
    }

    friend bool operator!=(const Line& lhs, const Line& rhs)
    {
      return !(lhs == rhs);
    }
  };
} // ECExtentCaches

#endif //ECEXTENTCACHE_H
