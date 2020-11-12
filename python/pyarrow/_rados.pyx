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

cdef class RadosDatasetFactoryOptions(_Weakrefable):
    cdef:
        CRadosDatasetFactoryOptions rados_factory_options

    __slots__ = ()

    def __init__(self, pool_name='rados_pool',
                 conf_path='/etc/ceph/ceph.conf',
                 object_list=[],
                 user_name='user',
                 cluster_name='cluster',
                 flags=0,
                 cls_name='rados',
                 cls_method='read'
                 ):
        self.rados_factory_options.pool_name_ = tobytes(pool_name)
        self.rados_factory_options.object_vector_ = [tobytes(s) for s in object_list]
        self.rados_factory_options.user_name_ = tobytes(user_name)
        self.rados_factory_options.cluster_name_ = tobytes(cluster_name)
        self.rados_factory_options.flags_ = flags
        self.rados_factory_options.cls_name_ = tobytes(cls_name)
        self.rados_factory_options.cls_method_ = tobytes(cls_method)

    cdef inline CRadosDatasetFactoryOptions unwrap(self):
        return self.rados_factory_options

cdef class RadosDataset(Dataset):
    cdef:
        CRadosDataset* rados_dataset

    def __init__(self, Schema schema=None, RadosDatasetFactoryOptions rados_factory_options=None):
        cdef:
            CRadosDatasetFactoryOptions c_rados_factory_options
        if rados_factory_options is None:
            rados_factory_options = RadosDatasetFactoryOptions()
        c_rados_factory_options = rados_factory_options.unwrap()
        sp_schema = pyarrow_unwrap_schema(schema)
        result = CRadosDataset.Make(sp_schema, c_rados_factory_options)
        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.rados_dataset = <CRadosDataset*> sp.get()
