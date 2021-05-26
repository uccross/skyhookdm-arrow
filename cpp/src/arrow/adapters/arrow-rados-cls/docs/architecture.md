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

<p align="center">
<img src="./architecture.png" width="90%">
</p>

* **Storage Layer:** SkyhookDM is built on top of the Ceph storage system. Ceph allows extending its object storage interface, RADOS, with C++ plugins (built using the Ceph Object Class SDK) which inturn allows embedding application specific methods inside the Ceph OSDs for direct access and manipulation of objects within the storage layer. We leverage this feature of Ceph and extend RADOS by implementing Object Class methods that utilize Arrow APIs to scan objects containing Parquet binary data inside the storage layer. Since the Arrow APIs expect a file-like object to work on, we implement a random access interface between the Arrow access and RADOS layers. This random access interface is analogous to the `ObjectInputFile` interface in the Arrow S3FS module and provides a similar file-like view over RADOS objects.

* **Client Layer:** Apache Arrow provides a `Dataset` API which provides a dataset abstraction over a collection of files in different storage backend like S3 and HDFS and allows scanning them. The Dataset API supports scanning files of different formats via its `FileFormat` abstraction. We extend the `ParquetFileFormat` API to create a `RadosParquetFileFormat` API which when plugged into the `Dataset` API enables offloading Parquet dataset scans to the storage layer. In SkyhookDM, we store datasets in the Ceph filesystem, CephFS, to utilize the filesystem metadata support it provides via Ceph Metadata Servers (Ceph MDS) for doing dataset discovery. While scanning, we leverage the filesystem metadata, especially the striping strategy information, to translate filenames in CephFS to object IDs in RADOS and call Object Class methods on these objects, essentially bypassing the filesystem layer.

# Lifetime of a Dataset in SkyhookDM

* **Write Path:** Datasets containing Parquet files or a directory heirarchy of Parquet files are written to a CephFS mount. While writing, each Parquet file is splitted into several smaller Parquet files of size `<= 128 MB`. We configure the stripe unit in CephFS to be 128 MB to ensure a `1:1 mapping` between a file and an object. The file layout is shown in the figure below. The reason behing choosing 128 MB as the stripe size is because Ceph doesn't perform well with objects any larger than 128 MB. Also, some of our performance experiments have shown most optimal performance with 128 MB Parquet files. Once the Parquet files are written to CephFS, they are ready to be scanned via the `RadosParquetFileFormat`. 

<p align="center">
<img src="./filelayout.png" width="80%">
</p>

* **Read Path:** At the time of scanning, when the `Execute` method is called on a `ScanTask`, first the size of the target file is read via a `stat` system call and the `ScanOptions` containing the scan `Expression` and `Projection Schema` is serialized into a `ceph::bufferlist` along with the file size for sending it to the storage layer. Next, the `DirectObjectAccess` interface is invoked with the serialized `ScanOptions` to scan the file within the Ceph OSDs bypassing the filesystem layer. Inside the `DirectObjectAccess` layer, the file inode value is converted to the corresponding object ID in RADOS and then using the `librados` library, a CLS method call is invoked over the object. Inside the CLS method, first the `ScanOptions` is deserialized back into the `Expression` and `Schema` objects. Then the `RandomAccessObject` interface is intialized over the object to get a file-like instance which is plugged into the `ParquetFileFragment` API for scanning the object. The resultant Arrow `Table` is written into an LZ4 compressed Arrow IPC buffer and is sent back to the client. Finally, on the client, the buffer is decompressed and the resulting `RecordBatches` are returned.
