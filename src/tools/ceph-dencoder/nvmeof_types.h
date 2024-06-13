// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_NVMEOF_TYPES_H
#define CEPH_NVMEOF_TYPES_H

#ifdef WITH_NVMEOF_GATEWAY_MONITOR_CLIENT

#include "mon/NVMeofGwTypes.h"
TYPE(NvmeGwMonClientStates)
TYPE(NvmeGwTimers)
TYPE(NvmeGwMonStates)
#include "messages/MNVMeofGwBeacon.h"
TYPE(MNVMeofGwBeacon)
#include "messages/MNVMeofGwMap.h"
TYPE(MNVMeofGwMap)

#endif // WITH_NVMEOF_GATEWAY_MONITOR_CLIENT

#endif // CEPH_NVMEOF_TYPES_H
