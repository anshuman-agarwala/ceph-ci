#!/usr/bin/env bash
set -ex

echo "getting blogbench"
wget https://github.com/jedisct1/Blogbench/releases/download/1.2/blogbench-1.2.tar.bz2
#cp /home/gregf/src/blogbench-1.0.tar.bz2 .
tar -xvf blogbench-1.2.tar.bz2
cd blogbench-1.2/
echo "making blogbench"
./configure
make
cd src
mkdir blogtest_in
echo "running blogbench"
./blogbench -d blogtest_in
