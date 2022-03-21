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

from pyarrow.lib cimport *
from pyarrow.lib import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_dataset cimport *

from pyarrow.lib cimport _Weakrefable

cdef extern from "skyhook/client/file_skyhook.h" \
        namespace "skyhook" nogil:
    cdef struct CRadosConnCtx "skyhook::RadosConnCtx":
        CRadosConnCtx()
        c_string ceph_config_path
        c_string ceph_data_pool
        c_string ceph_user_name
        c_string ceph_cluster_name
        c_string ceph_cls_name

cdef extern from "skyhook/client/file_skyhook.h" \
        namespace "skyhook" nogil:
    cdef cppclass CSkyhookFileFormat \
        "skyhook::SkyhookFileFormat"(
            CFileFormat):
        CSkyhookFileFormat(
            shared_ptr[CRadosConnCtx] ctx,
            c_string file_format
        )