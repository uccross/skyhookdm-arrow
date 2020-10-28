#!/bin/bash

cp ../../../../debug1/debug/libcls_arrow* /usr/lib64/rados-classes/
cp ../../../../debug1/debug/libarrow* /usr/lib64/rados-classes/
scripts/micro-osd.sh test-cluster /etc/ceph
build/bin/cls_arrow_test