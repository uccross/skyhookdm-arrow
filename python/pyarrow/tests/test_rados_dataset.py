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

import pyarrow as pa
import pytest
try:
    import pyarrow.rados as rados
except ImportError:
    rados = None

try:
    import pyarrow.dataset as ds
except ImportError:
    ds = None

@pytest.mark.rados
def test_rados_dataset():

    dataset = ds.dataset(
        source= "rados://localhost/ceph/test-pool?ids={}&conf_path={}".format(
            str(['obj.0', 'obj.1', 'obj.2', 'obj.3']),
            '/etc/ceph/ceph.conf'
        ),
        schema= pa.schema([
            pa.field('f1', pa.int64()),
            pa.field('f2', pa.int64())
        ])
    )
    assert isinstance(dataset, rados.RadosDataset)