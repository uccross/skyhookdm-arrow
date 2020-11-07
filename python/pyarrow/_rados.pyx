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

from pyarrow._dataset cimport Dataset, pyarrow_unwrap_schema
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.lib import frombytes, tobytes
from pyarrow.lib cimport *

cdef class RadosFormat(_Weakrefable):
    cdef:
        CRadosFormat rados_format

    __slots__ = ()

    def __init__(self, pool_name='rados_pool',
                 object_list=[],
                 user_name='user',
                 cluster_name='cluster',
                 flags=0,
                 cls_name='rados',
                 cls_method='read'
                 ):
        self.rados_format.pool_name_ = tobytes(pool_name)
        self.rados_format.object_vector_ = [tobytes(s) for s in object_list]
        self.rados_format.user_name_ = tobytes(user_name)
        self.rados_format.cluster_name_ = tobytes(cluster_name)
        self.rados_format.flags_ = flags
        self.rados_format.cls_name_ = tobytes(cls_name)
        self.rados_format.cls_method_ = tobytes(cls_method)

    cdef inline CRadosFormat unwrap(self):
        return self.rados_format

cdef class RadosDataset(Dataset):
    cdef:
        CRadosDataset* rados_dataset

    def __init__(self, conf_path, RadosFormat format=None, Schema schema=None):
        cdef:
            CRadosFormat c_format
        if format is None:
            format = RadosFormat()
        c_format = format.unwrap()
        sp_schema = pyarrow_unwrap_schema(schema)
        result = CRadosDataset.Make(sp_schema, tobytes(conf_path), c_format)
        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.rados_dataset = <CRadosDataset*> sp.get()
