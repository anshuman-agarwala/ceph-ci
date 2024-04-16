#!/usr/bin/env bash

set -ex
sudo eval "echo -n 'module ceph +mp' > /sys/kernel/debug/dynamic_debug/control"
dbench 1
dbench 10
