//
// Created by root on 10/17/24.
//

#include "ECExtentCache.h"

#include "ECUtil.h"

using namespace std;
using namespace ECUtil;

namespace ECExtentCache {

  uint64_t Object::free(uint64_t offset, uint64_t length)
  {
    uint64_t old_size = cache.size();
    cache.erase_stripe(offset, length);

    if (line_count == 0) {
      ceph_assert(cache.empty());
      pg.objects.erase(oid);
    }

    return old_size - cache.size();
  }

  void Object::cache_maybe_ready()
  {
    if (waiting_ops.empty())
      return;

    OpRef op = waiting_ops.front();
    if (cache.contains(op->reads))
    {
      op->result = cache.intersect(op->reads);
      op->complete = true;
      op->cache_ready_cb.release()->complete(op);
    }
  }

  void Object::request(OpRef &op)
  {
    /* else add to read */
    if (op->reads) {
      for (auto &&[shard, eset]: *(op->reads)) {
        extent_set request = eset;
        if (cache.contains(shard)) request.subtract(cache.get_extent_map(shard).get_interval_set());
        if (reading.contains(shard)) request.subtract(reading.at(shard));
        if (writing.contains(shard)) request.subtract(writing.at(shard));

        if (!request.empty()) {
          requesting[shard].insert(request);
        }
      }
    }

    // Store the set of writes we are doing in this IO after subtracting the previous set.
    // We require that the overlapping reads and writes in the requested IO are either read
    // or were written by a previous IO.
    for (auto &&[shard, eset]: op->writes) {
      if (op->writes.contains(shard)) writing[shard].insert(op->writes.at(shard));
    }

    waiting_ops.emplace_back(op);
    cache_maybe_ready();
    send_reads();
  }

  void Object::send_reads()
  {
    if (!reading.empty() || requesting.empty())
      return; // Read busy

    reading.swap(requesting);
    pg.backend_read.backend_read(oid, reading);
  }

  uint64_t Object::read_done(shard_extent_map_t const &buffers)
  {
    reading.clear();
    uint64_t size_change = insert(buffers);
    send_reads();
    return size_change;
  }

  uint64_t Object::write_done(OpRef &op, shard_extent_map_t const &buffers)
  {
    ceph_assert(op == waiting_ops.front());
    waiting_ops.pop_front();
    uint64_t size_change = insert(buffers);
    return size_change;
  }

  uint64_t Object::insert(shard_extent_map_t const &buffers)
  {
    uint64_t old_size = cache.size();
    cache.insert(buffers);
    for (auto && [shard, emap] : buffers.get_extent_maps()) {
      if (writing.contains(shard)) {
        writing.at(shard).subtract(buffers.get_extent_map(shard).get_interval_set());
        if(writing.at(shard).empty()) {
          writing.erase(shard);
        }
      }
    }
    cache_maybe_ready();

    return cache.size() - old_size;
  }

  void PG::request(OpRef &op, hobject_t const &oid, std::optional<std::map<int, extent_set>> const &to_read, std::map<int, extent_set> const &write)
  {
    op->oid = oid;
    op->reads = to_read;
    op->writes = write;

    if (!objects.contains(op->oid)) {
      objects.emplace(op->oid, Object(*this, op->oid));
    }
    lru.pin(op, sinfo.get_chunk_size(), objects.at(op->oid));
    objects.at(op->oid).request(op);
  }

  void PG::read_done(hobject_t const& oid, shard_extent_map_t const&& update)
  {
    uint64_t size = objects.at(oid).read_done(update);
    lru.inc_size(size);
  }

  void PG::write_done(OpRef &op, shard_extent_map_t const&& update)
  {
    uint64_t size_added = objects.at(op->oid).write_done(op, update);
    lru.inc_size(size_added);
  }

  void LRU::pin(OpRef &op, uint64_t alignment, Object &object)
  {
    mutex.lock();
    extent_set eset;
    for (auto &&[_, e]: op->writes) eset.insert(e);

    eset.align(alignment);

    for (auto &&[start, len]: eset ) {
      for (uint64_t to_pin = start; to_pin < start + len; to_pin += alignment) {
        Address a = Address(op->oid, to_pin);
        if (!lines.contains(a))
          lines.emplace(a, std::move(Line(object, a)));
        Line &l = lines.at(a);
        if (l.in_lru) lru.remove(l);
        l.in_lru = false;
        l.ref_count++;
      }
    }
    mutex.unlock();
  }

  void LRU::inc_size(uint64_t _size) {
    mutex.lock();
    size += _size;
    mutex.unlock();
  }

  void LRU::dec_size(uint64_t _size) {
    ceph_assert(size >= _size);
    size -= _size;
  }

  void PG::complete(OpRef &op) {
    lru.mutex.lock();
    map<hobject_t, list<uint64_t>> free_list = lru.unpin(op, sinfo.get_chunk_size());

    uint64_t size_delta = 0;

    for (auto &&[oid, offsets] : free_list) {
      for (auto offset : offsets) {
        size_delta += objects.at(oid).free(offset, sinfo.get_chunk_size());
      }
    }

    lru.dec_size(size_delta);
    lru.mutex.unlock();
  }

  map<hobject_t, list<uint64_t>> LRU::unpin(OpRef &op, uint64_t alignment)
  {
    extent_set eset;
    for (auto &&[_, e]: op->writes) eset.insert(e);
    eset.align(alignment);

    for (auto &&[start, len]: eset ) {
      for (uint64_t to_pin = start; to_pin < start + len; to_pin += alignment) {
        Line &l = lines.at(Address(op->oid, to_pin));
        ceph_assert(l.ref_count);
        if (!--l.ref_count) {
          l.in_lru = true;
          lru.emplace_back(l);
        }
      }
    }
    return free_maybe();
  }

  map<hobject_t, list<uint64_t>> LRU::free_maybe() {
    map<hobject_t, list<uint64_t>> to_erase;
    while (max_size < size && !lru.empty())
    {
      Line &l = lru.front();
      to_erase[l.address.oid].emplace_back(l.address.offset);
      lru.pop_front();
      lines.erase(l.address);
    }

    return to_erase;
  }

  bool PG::idle(hobject_t &oid) const
  {
    return objects.contains(oid) && objects.at(oid).waiting_ops.empty();
  }
} // ECExtentCache