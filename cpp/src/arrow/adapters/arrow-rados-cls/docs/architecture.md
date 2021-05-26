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

# Architecture

## Storage Layer

SkyhookDM is built on top of the Ceph storage system. Ceph allows extending its object storage interface, RADOS easily with C++ plugins which allows embedding application specific functions for direct access and manipulation of objects from within the storage layer. We embed Arrow libraries inside the storage layer and use Arrow APIs to scan objects containing tabular data. Currently, we support only Parquet files.


## Client Layer

Apache Arrow provides a Dataset API that allows scanning a dataset composed of multiple files containing tabular data. The Dataset API supports scanning files of different formats via the FileFormat abstraction. Since, offload the Parquet file scanning functionality to the storage layer, we extend the ParquetFileFormat API to create a RadosParquetFileFormat API which when plugged in to the Dataset API offloads the scanning. 


Since discovering and scanning datasets requires metadata support, we use the Ceph Filesystem layer, CephFS for file R/W which provides scalable metadata support via the Ceph Metadata Servers (MDS). 

## Lifetime of a Scan

1. **Write Path:** While writing a dataset containing Parquet files, every Parquet file is splitted into several Parquet files of size `<= 128 MB`. We also configure the stripe unit in CephFS to be 128 MB to ensure a `1:1 mapping` between a file and an object. The reason behing choosing 128 MB as the stripe size is because Ceph doesn't perform well with objects any larger than 128 MB. Also, some of our performance experiments have shown most optimal performance with 128 MB Parquet files.

2. **Read Path:** While reading, when the `Execute` method is called on a `ScanTask`, first the size of the target file fragment is read via a `stat` system call. Then the `ScanOptions` is serialized into a ceph::bufferlist along with the fragment for sending it to the storage layer. After the serialized `ScanOptions` is ready, the `DirectObjectAccess` interface is invoked. Inside the `DirectObjectAccess` layer, the file inode `st_ino` is converted to the corresponding object ID in RADOS. Then using the `librados` API, a CLS function call is launced over the concerned object. Inside the CLS `scan_op` function, first the `ScanOptions` is deserialized back into the `Expression` and `Schema` objects. Then the RandomAccessObject interface is intialized over the object to get a file-like interface over the object. This instance is plugged into the ParquetFileFragment API and the object is scanned. The resultant `Table` is written into an LZ4 compressed Arrow IPC buffer and is sent back to the client via a `ceph::bufferlist`. Lastly, the buffer is decompressed on the client and the resultant RecordBatches are returned.
