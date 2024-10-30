// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "osd/PGTransaction.h"
#include "osd/ECTransaction.h"
#include "osd/ECBackend.h"

#include "test/unit.cc"

struct mydpp : public DoutPrefixProvider {
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "foo"; }
  CephContext *get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return ceph_subsys_osd; }
} dpp;

#define dout_context g_ceph_context

struct ECTestOp : ECCommon::RMWPipeline::Op {
  PGTransactionUPtr t;
};

TEST(ectransaction, two_writes_separated_append)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a, b;
  a.append_zero(565760);
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});
  b.append_zero(2437120);
  op.buffer_updates.insert(669856, b.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{b, 0});

  ECUtil::stripe_info_t sinfo(2, 8192, 0, std::vector<int>(0));

  ECTransaction::WritePlanObj plan(
    op,
    sinfo,
    std::nullopt,
    std::nullopt,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr);

  generic_derr << "plan " << plan << dendl;

  ASSERT_FALSE(plan.to_read);
  ASSERT_EQ(2u, plan.will_write.size());
}

TEST(ectransaction, two_writes_separated_misaligned_overwrite)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a, b;
  a.append_zero(565760);
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});
  b.append_zero(2437120);
  op.buffer_updates.insert(669856, b.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{b, 0});

  ECUtil::stripe_info_t sinfo(2, 8192, 0, std::vector<int>(0));
  object_info_t oi;
  oi.size = 3112960;

  ECTransaction::WritePlanObj plan(
  op,
  sinfo,
  oi,
  oi,
  std::nullopt,
  ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
  nullptr);

  generic_derr << "plan " << plan << dendl;


  ASSERT_EQ(2u, (*plan.to_read).size());
  ASSERT_EQ(2u, plan.will_write.size());
}

