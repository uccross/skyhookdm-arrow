#!/bin/bash

cp ../../../../debug/debug/libcls_arrow* /usr/lib64/rados-classes/
scripts/micro-osd.sh test-cluster /etc/ceph
build/bin/cls_arrow_test