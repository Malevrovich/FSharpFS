#!/bin/bash

set -e

dd if=/dev/random of=/tmp/fsfs_test_data bs=4096 count=100

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/small bs=1 count=1
dd if=/tmp/fsfs_test_data of=/tmp/fsfs/small bs=1 count=10
dd if=/tmp/fsfs_test_data of=/tmp/fsfs/small bs=1 count=100
dd if=/tmp/fsfs_test_data of=/tmp/fsfs/small bs=1 count=1000

rm /tmp/fsfs/small

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/medium bs=4096 count=1
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=4096 count=1
cmp /tmp/fsfs_test_cut /tmp/fsfs/medium
rm /tmp/fsfs_test_cut

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/medium bs=4096 count=2
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=4096 count=2
cmp /tmp/fsfs_test_cut /tmp/fsfs/medium
rm /tmp/fsfs_test_cut

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/medium bs=4096 count=8
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=4096 count=8
cmp /tmp/fsfs_test_cut /tmp/fsfs/medium
rm /tmp/fsfs_test_cut

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/medium bs=4096 count=100
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=4096 count=100
cmp /tmp/fsfs_test_cut /tmp/fsfs/medium
rm /tmp/fsfs_test_cut

rm /tmp/fsfs/medium

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/medium bs=8K count=1
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=8K count=1
cmp /tmp/fsfs_test_cut /tmp/fsfs/medium
rm /tmp/fsfs_test_cut

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/medium bs=128K count=2
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=128K count=2
cmp /tmp/fsfs_test_cut /tmp/fsfs/medium
rm /tmp/fsfs_test_cut

rm /tmp/fsfs/medium

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/unaligned bs=500 count=1
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=500 count=1
cmp /tmp/fsfs_test_cut /tmp/fsfs/unaligned
rm /tmp/fsfs_test_cut

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/unaligned bs=5000 count=1
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=5000 count=1
cmp /tmp/fsfs_test_cut /tmp/fsfs/unaligned
rm /tmp/fsfs_test_cut

dd if=/tmp/fsfs_test_data of=/tmp/fsfs/unaligned bs=5000 count=55
dd if=/tmp/fsfs_test_data of=/tmp/fsfs_test_cut bs=5000 count=55
cmp /tmp/fsfs_test_cut /tmp/fsfs/unaligned
rm /tmp/fsfs_test_cut

rm /tmp/fsfs/unaligned

echo "Success!"