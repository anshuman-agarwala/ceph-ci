#!/usr/bin/env bash

set -ex
echo -n 'module ceph +p' > /sys/kernel/debug/dynamic_debug/control
dbench 1
dbench 10
