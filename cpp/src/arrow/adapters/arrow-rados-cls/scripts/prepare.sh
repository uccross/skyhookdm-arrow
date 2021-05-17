#!/bin/bash
set -ex

apt update
apt install -y python3 python3-pip python3-venv python3-numpy cmake libradospp-dev rados-objclass-dev ceph-osd ceph-mon ceph-common ceph-mgr ceph-mds ceph-fuse
