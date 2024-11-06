//
// Created by root on 10/17/24.
//

#include "ECExtentCache.h"

#include "ECUtil.h"

using namespace std;
using namespace ECUtil;

namespace ECExtentCache {

  void PG::cache_maybe_ready()
  {
    while (!waiting_ops.empty()) {
      OpRef op = waiting_ops.front();
      if (!op->object.cache.contains(op->reads))
        return;

      op->result = op->object.cache.intersect(op->reads);
      op->complete = true;
      op->cache_ready_cb.release()->complete(op);

      /* The front of waiting ops is removed if write_done() is called. */
      if (op == waiting_ops.front())
        return;
    }
  }

  void Object::request(OpRef &op)
  {
    uint64_t alignment = sinfo.get_chunk_size();
    extent_set eset = op->get_pin_eset(sinfo.get_chunk_size());

    for (auto &&[start, len]: eset ) {
      for (uint64_t to_pin = start; to_pin < start + len; to_pin += alignment) {
        if (!lines.contains(to_pin))
          lines.emplace(to_pin, Line(*this, to_pin));
        Line &l = lines.at(to_pin);
        if (l.in_lru) pg.lru.lru.remove(l);
        l.in_lru = false;
        l.ref_count++;
      }
    }

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
    active_ios++;

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

  void Object::check_buffers_pinned(shard_extent_map_t const &buffers) {
    extent_set eset = buffers.get_extent_superset();
    uint64_t alignment = sinfo.get_chunk_size();
    eset.align(alignment);

    for (auto &&[start, len]: eset ) {
      for (uint64_t to_pin = start; to_pin < start + len; to_pin += alignment) {
        ceph_assert(lines.contains(to_pin));
      }
    }
  }

  void Object::check_cache_pinned() {
    check_buffers_pinned(cache);
  }

  uint64_t Object::insert(shard_extent_map_t const &buffers)
  {
    check_buffers_pinned(buffers);
    check_cache_pinned();

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

    return cache.size() - old_size;
  }

  void Object::unpin(OpRef &op) {
    uint64_t alignment = sinfo.get_chunk_size();
    extent_set eset = op->get_pin_eset(alignment);

    for (auto &&[start, len]: eset ) {
      for (uint64_t to_pin = start; to_pin < start + len; to_pin += alignment) {
        Line &l = lines.at(to_pin);
        ceph_assert(l.ref_count);
        if (!--l.ref_count) {
          l.in_lru = true;
          pg.lru.lru.emplace_back(l);
        }
      }
    }

    ceph_assert(active_ios > 0);
    active_ios--;
    delete_maybe();
  }

  void Object::delete_maybe() {
    if (lines.empty() && active_ios == 0) {
      ceph_assert(cache.empty());
      pg.objects.erase(oid);
    }
  }

  uint64_t Object::erase_line(Line &line) {
    uint64_t size_delta = cache.size();
    cache.erase_stripe(line.offset, sinfo.get_chunk_size());
    lines.erase(line.offset);
    size_delta -= cache.size();
    check_cache_pinned();
    delete_maybe();
    return size_delta;
  }

  OpRef PG::request(GenContextURef<OpRef &> && ctx, hobject_t const &oid, std::optional<std::map<int, extent_set>> const &to_read, std::map<int, extent_set> const &write, uint64_t projected_size)
  {
    lru.mutex.lock();

    if (!objects.contains(oid)) {
      objects.emplace(oid, Object(*this, oid));
    }
    OpRef op = std::make_shared<Op>(std::move(ctx), objects.at(oid));

    op->reads = to_read;
    op->writes = write;
    op->object.projected_size = projected_size;
    op->object.request(op);

    waiting_ops.emplace_back(op);

    cache_maybe_ready();
    lru.mutex.unlock();

    return op;
  }

  void PG::read_done(hobject_t const& oid, shard_extent_map_t const&& update)
  {
    lru.mutex.lock();
    uint64_t size = objects.at(oid).read_done(update);
    lru.inc_size(size);
    cache_maybe_ready();
    lru.mutex.unlock();
  }

  void PG::write_done(OpRef &op, shard_extent_map_t const&& update)
  {
    ceph_assert(ceph_mutex_is_locked_by_me(lru.mutex));
    ceph_assert(op == waiting_ops.front());
    waiting_ops.pop_front();
    uint64_t size_added = op->object.insert(update);
    lru.inc_size(size_added);
  }

  uint64_t PG::get_projected_size(hobject_t const &oid) {
    return objects.at(oid).projected_size;
  }

  bool PG::contains_object(hobject_t const &oid) {
    return objects.contains(oid);
  }

  void LRU::inc_size(uint64_t _size) {
    ceph_assert(ceph_mutex_is_locked_by_me(mutex));
    size += _size;
  }

  void LRU::dec_size(uint64_t _size) {
    ceph_assert(size >= _size);
    size -= _size;
  }

  void PG::complete(OpRef &op) {
    lru.mutex.lock();
    op->object.unpin(op);
    lru.free_maybe();
    lru.mutex.unlock();
  }

  void PG::discard_lru() {
    lru.mutex.lock();
    lru.discard();
    lru.mutex.unlock();
  }

  void LRU::free_to_size(uint64_t target_size) {
    while (target_size < size && !lru.empty())
    {
      Line l = lru.front();
      lru.pop_front();
      dec_size(l.object.erase_line(l));
    }
  }

  void LRU::free_maybe() {
    free_to_size(max_size);
  }

  void LRU::discard() {
    free_to_size(0);
  }

  bool PG::idle(hobject_t &oid) const
  {
    return objects.contains(oid) && waiting_ops.empty();
  }

  extent_set Op::get_pin_eset(uint64_t alignment) {
    extent_set eset;
    for (auto &&[_, e]: writes) eset.insert(e);
    if (reads) for (auto &&[_, e]: *reads) eset.insert(e);
    eset.align(alignment);

    return eset;
  }
} // ECExtentCache