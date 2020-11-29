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

@pytest.mark.rados
def test_rados_dataset():
    import urllib
    import pyarrow.rados as rados
    import rados as librados
    import pyarrow.dataset as ds

    cluster = librados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    cluster.create_pool('test-pool')

    dataset = ds.dataset(
        source= "rados:///etc/ceph/ceph.conf?cluster=ceph&pool=test-pool&ids={}".format(
            urllib.parse.quote(str(['obj.0', 'obj.1', 'obj.2', 'obj.3']), safe='')
        ),
        schema= pa.schema([
            pa.field('a', pa.int8()),
            pa.field('b', pa.float64())          
        ])
    )
    assert isinstance(dataset, rados.RadosDataset)

    cluster.delete_pool('test-pool')

@pytest.mark.rados
def test_rados_url():
    import urllib
    import pyarrow.rados as rados
    import rados as librados
    import pyarrow.dataset as ds

    conf = '/etc/ceph/ceph.conf'
    cluster = 'ceph'
    pool = 'test-pool'
    ids = ['obj.0', 'obj.1', 'obj.2', 'obj.3']
    url = "rados://{}?cluster={}&pool={}&ids={}".format(
            conf,
            cluster,
            pool,
            urllib.parse.quote(str(ids), safe='')
        )

    generated_url = rados.generate_uri(ceph_config_path=conf, cluster=cluster, pool=pool, objects=ids)

    rados_factory_options = rados.parse_uri(generated_url)
    assert rados_factory_options.ceph_config_path == conf
    assert rados_factory_options.cluster_name == cluster
    assert rados_factory_options.pool_name == pool
    assert rados_factory_options.objects == ids 
