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


import urllib
import os.path
import ast
try:
    from pyarrow._rados import RadosDataset, RadosDatasetFactoryOptions
except ImportError:
    raise ImportError(
                "The pyarrow installation is not built with support for rados."
            )

_RADOS_URI_SCHEME = 'rados'

def generate_uri(host='localhost', cluster='ceph', pool='test_pool', conf_path='/etc/ceph/ceph.conf', username='client.admin', object_list=[], flags=0):
    host = urllib.parse.quote(host, safe='')
    cluster = urllib.parse.quote(cluster, safe='')
    pool = urllib.parse.quote(pool, safe='')
    params = {}
    params['ids'] = object_list
    params['conf_path'] = conf_path
    params['username'] = username
    params['flags'] = flags
    query = urllib.parse.urlencode(params)
    query = '?' + query
    return "{}://{}/{}/{}{}".format(_RADOS_URI_SCHEME, host, cluster, pool, query)


def parse_uri(uri):
    if not is_valid_rados_uri(uri):
        return None
    url_object = urllib.parse.urlparse(uri)
    host = urllib.parse.unquote(url_object.netloc)
    path_components = _parse_path(url_object.path)
    cluster = urllib.parse.unquote(path_components[0])
    pool = urllib.parse.unquote(path_components[0])
    params = urllib.parse.parse_qs(url_object.query)
    rados_factory_options = RadosDatasetFactoryOptions(cluster_name=cluster, pool_name=pool)
    if params['username']:
        rados_factory_options.user_name = params['username'][0]
    if params['conf_path']:
        rados_factory_options.conf_path = params['conf_path'][0]
    if params['ids']:
        ids = ast.literal_eval(params['ids'][0])
        rados_factory_options.object_list = ids
    if params['flags']:
        rados_factory_options.flags = int(params['flags'][0])
    return rados_factory_options



def is_valid_rados_uri(uri):
    url_object = urllib.parse.urlparse(uri)
    if url_object.scheme == _RADOS_URI_SCHEME:
        path_components = _parse_path(url_object.path)
        if len(path_components) == 2:
            return True
    return False


def _parse_path(path):
    path_components = []
    head = None
    tail = path
    while head != '':
        tail, head = os.path.split(tail)
        if head != '':
            path_components.insert(0, head)
    return path_components