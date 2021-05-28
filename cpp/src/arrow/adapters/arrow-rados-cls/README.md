<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# SkyhookDM-Arrow

Apache Arrow provides a `Dataset` API, which acts as an abstraction over a collection of files in different storage backend like S3 and HDFS. It supports different file formats like CSV and Parquet through the `FileFormat` API. In SkyhookDM, since we require to pushdown
compute operations into the Storage backend, we created a new file format on top of Parquet, namely a `RadosParquetFileFormat` which besides providing the benefits of Parquet, allows pushing down filter and projection operations into the storage backend to minimize data moved through the network.

# Getting Started

**NOTE:** Please make sure [docker](https://docs.docker.com/engine/install/ubuntu/) and [docker-compose](https://docs.docker.com/compose/install/) is installed.

* Clone the repository.
```bash
git clone --branch v0.1.1 https://github.com/uccross/arrow
```

* Run the `ubuntu-cls-demo` step in the docker-compose file. This step will start a single node Ceph cluster inside the container, mount CephFS, put sample data into CephFS, and open an example Jupyter notebook with PyArrow installed.
```bash
cd arrow/
docker-compose run --service-ports ubuntu-cls-demo
```

# Installation Instructions

## Installing SkyhookDM-Arrow with [Rook](https://rook.io) on Kubernetes

* Change the Ceph image tag in the Rook CRD [here](https://github.com/rook/rook/blob/master/cluster/examples/kubernetes/ceph/cluster.yaml#L24) to the image built from [this](./docker) dir (or you can quickly use `uccross/skyhookdm-arrow:vX.Y.Z` as the image tag) to change your Rook Ceph cluster to the `vX.Y.Z` version of SkyhookDM Arrow. 

* After the cluster is updated, we need to deploy a Pod with the PyArrow (with RadosParquetFileFormat API) library installed to start interacting with the cluster. This can be achieved by following these steps:

  1) Update the ConfigMap with configuration options to be able to load the arrow CLS plugins.
  ```bash
  kubectl apply -f cls.yaml
  ```

  2) Create a Pod with PyArrow pre-installed for connecting to the cluster and running queries. 
  ```bash
  kubectl apply -f client.yaml
  ```

  3) Create a CephFS on the Rook cluster.
  ```bash
  kubectl create -f filesystem.yaml
  ```
  
  4) Copy the Ceph configuration and Keyring from some OSD/MON Pod to the playground Pod.
  ```bash
  # copy the ceph config
  kubectl -n [namespace] cp [any-osd/mon-pod]:/var/lib/rook/[namespace]/[namespace].config ceph.conf
  kubectl -n [namespace] cp ceph.conf rook-ceph-playground:/etc/ceph/ceph.conf

  # copy the keyring
  kubectl -n [namespace] cp [any-osd/mon-pod]:/var/lib/rook/[namespace]/client.admin.keyring keyring
  kubectl -n [namespace] cp keyring rook-ceph-playground:/etc/ceph/keyring
  ```
  **NOTE:** You would need to change the keyring path in the ceph config to `/etc/ceph/keyring` manually.

  5) Check the connection to the cluster from the client Pod.
  ```bash
  # get a shell into the client pod
  kubectl -n [namespace] exec -it rook-ceph-playground bash

  # check the connection status
  $ ceph -s
  ```

  6) Now, install `ceph-fuse` and mount CephFS into some path in the client Pod using it. [In a later release 
  `ceph-fuse` will come installed in the SkyhookDM image itself.]

  ```bash
  yum install ceph-fuse
  mkdir -p /mnt/cephfs
  ceph-fuse --client_fs cephfs /mnt/cephfs 
  ```
  **NOTE:** The `client_fs` name can be different. Please check the filesystem.yaml file for the filesystem name you are using.

  4) Download some example dataset into `/path/to/cephfs/mount`. For example,
  ```bash
  cd /mnt/cephfs
  wget https://raw.githubusercontent.com/JayjeetAtGithub/zips/main/nyc.zip
  unzip nyc.zip
  ```

  4) Modify the [example python script](./example.py) according to your needs and execute.
  ```bash
  python3 example.py
  ```

## Deploy Ceph (+SkyhookDM) on CloudLab

A workflow for deploying Ceph on CloudLab using `geni-lib` and 
Ansible; assumes basic knowledge of how these two tools work.

The workflow in [`wf.yml`](./wf.yml) deploys Ceph on a 
[Cloudlab][cloudlab] allocation using [`ceph-ansible`][ca]. The 
workflow consists of the following steps:

  * **`allocate resources`**. Request for resources to CloudLab and 
    wait for them to be instantiated. The configuration is specified 
    in the [`geni/config.py`](./geni/config.py)

  * **`generate ansible inventory`**. Generates an Ansible inventory 
    out of the GENI manifest produced by the previous `allocate 
    resources` step. This is used by the subsequent `deploy` step.

  * **`deploy`**. Deploys Ceph by running a playbook from the 
    [`ansible/playbooks/`](./ansible/playbooks) folder. After this 
    step executes, the resulting `ceph.conf` file is placed in 
    [`ansible/fetch/`](./ansible/fetch).

  * **`download and extract skyhook library/download and extract skyhook-arrow library`**. Extracts the
    SkyhookDM and SkyhookDM-Arrow shared libraries from their respective Docker images into the [`ansible/files`](./ansible/files) directory.

  * **`deploy skyhook library`**. Copies the shared libraries from [`ansible/files`](./ansible/files) into the `/usr/lib64/rados-classes/` directory in the Ceph OSDs by running [this](./ansible/deploy-libcls_tabular.yml) playbook.

  * **`release resources`**. Releases all the resources spawned by the workflow.

In addition to the above, subsequent steps can be added to the 
workflow in order to run tests, benchmarks or other workloads that 
work on Ceph. Take a look at the [`test`](../../test) folder for 
examples. This workflow also includes a (commented out) `teardown` 
step that releases the allocated resources in CloudLab and can be 
invoked to release resources.

### Usage

To execute this workflow:

 1. Clone this repository or copy the contents of this folder into 
    your project:

    ```bash
    git clone https://github.com/uccross/skyhookdm-workflows
    cd skyhookdm-workflows/cloudlab
    ```

 2. Define the secrets expected by the workflow, declared in the 
    `secrets` attribute of steps:

    ```bash
    export GENI_FRAMEWORK=emulab-ch2
    export GENI_PROJECT=<project>
    export GENI_USERNAME=<username>
    export GENI_KEY_PASSPHRASE='<password>'
    export GENI_PUBKEY_DATA=$(cat ~/.ssh/id_rsa.pub | base64)
    export GENI_CERT_DATA=$(cat ~/.ssh/cloudlab.pem | base64)

    export ANSIBLE_SSH_KEY_DATA=$(cat ~/.ssh/id_rsa | base64)
    ```

    See [the GENI image README][gd] for more information about the 
    secrets related to the GENI steps; similarly [here][cad] for the 
    Ansible step. **NOTE:** the value of `GENI_PROJECT` is expected
    to be all lower case.

 3. Tweak the following to your needs:

     * The experiment name in the [GENI config 
       file](./geni/config.py). Changing the name of the experiment 
       avoids having another member of your project running the script 
       and resulting in name clashes.

     * The number of nodes, and how they are to be grouped in terms of 
       their roles (`osd`, `mon`, `rgw`, etc.). This is also specified 
       in the GENI [config file](./geni/config.py).

     * Specify in the `deploy` step which Ansible playbook is to be 
       executed (see the [`ansible/`](./ansible/) 
       folder. Multiple playbooks may exist, depending on what type of 
       deployment is needed (e.g. ).

     * Tweak Ceph variables for the cluster available in the 
       [`ansible/group_vars`](./ansible/group_vars) folder.

 4. Execute the workflow by doing:

    ```bash
    popper run -f wf.yml
    ```

For more information on Popper, visit 
<https://github.com/systemslab/popper>.

[cloudlab]: https://cloudlab.us
[ca]: https://github.com/ceph/ceph-ansible
[gd]: https://github.com/getpopper/library/tree/master/geni
[cad]: https://github.com/getpopper/library/tree/master/ansible
[ca-docs]: http://docs.ceph.com/ceph-ansible/master/

## Installing SkyhookDM on bare-metal in general

1. If you don't already have a Ceph cluster, please follow [this](https://blog.risingstack.com/ceph-storage-deployment-vm/) guide to create one. 

2. Create and mount CephFS at some path, for example `/mnt/cephfs`.

2. Build and install SkyhookDM and [PyArrow](https://pypi.org/project/pyarrow/) (with Rados Parquet extensions) using [this](https://github.com/JayjeetAtGithub/skyhook-perf-experiments/blob/master/deployment_scripts/skyhook.sh) script.

3. Update your Ceph configuration file with this line.
```
osd class load list = *
```

4. Restart the Ceph OSDs to load the changes.

# Interacting with SkyhookDM

1. Write some [Parquet](https://parquet.apache.org/) files in the CephFS mount.

2. Write a client script and get started with querying datasets in SkyhookDM. An example script is given below.
```python
import pyarrow.dataset as ds

format_ = ds.RadosParquetFileFormat("/path/to/cephconfig", "cephfs-data-pool-name")
dataset_ = ds.dataset("file:///mnt/cephfs/dataset", format=format_)
print(dataset_.to_table())
```
 
# Salient Features

* Enables pushing down filters, projections, compute operations to the Storage backend for minimal data transfer over the network.

* Allows storing data in Parquet files for minimizing Disk I/O though predicate and projection pushdown.

* Plugs-in seamlessly into the Arrow Dataset API and leverages all its functionality like dataset discovering,  partition pruning, etc.

* Minimal overhead in requirements: 
    1) Requires CephFS to be mounted. 
    2) Requires using the `SplittedParquetWriter` API to write arrow Tables.

* Built on top of latest Ceph v15.2.x.

# Code Structure

### Client side - C++

* `cpp/src/arrow/dataset/file_rados_parquet.h`: This file contains the definitions of 3 APIs. The `RadosCluster` , `DirectObjectAccess`, and the `RadosParquetFileFormat`. The `RadosCluster` API helps create a connection to the Ceph cluster and provides a handle to the cluster that can be passed around. The `DirectObjectAccess` API provides abstractions for converting filenames in CephFS to object IDs in the Object store and allows interacting with the objects directly. The `RadosParquetFileFormat` API takes in the direct object access construct as input and contains the logic of pushing down scans to the underlying objects that make up a file.

* `cpp/src/arrow/dataset/rados.h`: Contains a wrapper for the `librados` SDK for exposing `librados` methods like `init2`, `connect`, `stat`, `ioctx_create`, and `exec` which are required for establishing the connection to the Ceph cluster and for operating on objects directly. 

* `cpp/src/arrow/dataset/rados_utils.h`: Contains utility functions for (de)serializing query options, query results, etc. Currently, we serialize all the expressions and schemas into a `ceph::bufferlist`, but in a later release, we plan to use a Flatbuffer schema for making the scan options more scalable.

### Client side - Python

* `python/pyarrow/_rados.pyx/_rados.pxd`: Contains Cython bindings to the `RadosParquetFileFormat` C++ API.

* `python/pyarrow/rados.py`: This file contains the definition of the `SplittedParquetWriter`. It is completely implemented in Python.

### Storage side

* `cpp/src/arrow/adapters/arrow-rados-cls/cls_arrow.cc`: Contains the Rados objclass functions and APIs for interacting with objects in the OSDs. Also, it includes a `RandomAccessObject` API to give a random access file view of objects for allowing operations like reading byte ranges, seeks, tell, etc. 

# Setting up the development environment using docker

**NOTE:** Please make sure [docker](https://docs.docker.com/engine/install/ubuntu/) and [docker-compose](https://docs.docker.com/compose/install/) is installed.

1. Clone the repository.
```bash
git clone --branch rados-dataset-dev https://github.com/uccross/arrow
```

2. Install [Archery](https://arrow.apache.org/docs/developers/archery.html#), the daily development tool by Apache Arrow community.
```bash
cd arrow/
pip install -e dev/archery
```

2. Build and test the C++ client.
```bash
export UBUNTU=20.04
archery docker run ubuntu-cpp-cls
```

3. Build and test the Python client.
```bash
export UBUNTU=20.04
archery docker run ubuntu-python-cls
```

# Setting up development environemnt on bare-metal

1. Clone the repository.
```bash
git clone --branch rados-dataset-dev https://github.com/uccross/arrow
```

2. Run the [`build.sh`](./scripts/build.sh) and [`test.sh`](./scripts/test.sh) scripts from the repository root.
```bash
cd arrow/

# copy the scripts to the repository root
cp -r cpp/src/arrow/adapters/arrow-rados-cls/scripts/* .

# install the required packages
./prepare.sh 

# run build.sh and test.sh iteratively
./build.sh
./test.sh
```
