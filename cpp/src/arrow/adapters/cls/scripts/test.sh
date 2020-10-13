#!/bin/bash
set -e

cp build/lib/libcls_arrow.so* /usr/lib64/rados-classes/
scripts/micro-osd.sh test-cluster /etc/ceph
build/bin/cls_arrow_test
