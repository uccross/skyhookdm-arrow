# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: language_level = 3

from pyarrow._dataset cimport FileFormat
from pyarrow.lib cimport *
from pyarrow.lib import frombytes, tobytes
from pyarrow.includes.libarrow_dataset_skyhook cimport *

cdef class SkyhookFileFormat(FileFormat):
    """
    A FileFormat implementation that offloads the fragment
    scan operations to the Ceph OSDs.
    Parameters
    ---------
    file_format: The underlying file format to use.
    ceph_config_path: The path to the Ceph config file.
    data_pool: Name of the CephFS data pool.
    user_name: The username accessing the Ceph cluster.
    cluster_name: Name of the cluster.
    """
    cdef:
        CSkyhookFileFormat* skyhook_format

    def __init__(
        self,
        file_format="parquet",
        ceph_config_path="/etc/ceph/ceph.conf",
        data_pool="cephfs_data",
        user_name="client.admin",
        cluster_name="ceph",
        cls_name="arrow"
    ):  
        shared_ptr[CRadosConnCtx] ctx
        ctx.ceph_config_path = ceph_config_path
        ctx.data_pool = data_pool
        ctx.user_name = user_name
        ctx.cluster_name = cluster_name
        ctx.cls_name = cls_name

        self.init(shared_ptr[CFileFormat](
            new CSkyhookFileFormat(
                ctx,
                tobytes(file_format)
            )
        ))

    cdef void init(self, const shared_ptr[CFileFormat]& sp):
        FileFormat.init(self, sp)
        self.skyhook_format = <CSkyhookFileFormat*> sp.get()
