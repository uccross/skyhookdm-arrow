// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This API is EXPERIMENTAL.
#define _FILE_OFFSET_BITS 64

#pragma once

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/api.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT RadosCluster {
 public:
  explicit RadosCluster(std::string ceph_config_path_, std::string data_pool_, std::string user_name_, std::string cluster_name_)
      : data_pool(data_pool_),
        user_name(user_name_),
        cluster_name(cluster_name_),
        ceph_config_path(ceph_config_path_),
        flags(0),
        cls_name("arrow"),
        rados(new RadosWrapper()),
        ioCtx(new IoCtxWrapper()) {}

  ~RadosCluster() { Shutdown(); }

  Status Connect() {
    if (rados->init2(user_name.c_str(), cluster_name.c_str(), flags))
      return Status::Invalid("librados::init2 returned non-zero exit code.");

    if (rados->conf_read_file(ceph_config_path.c_str()))
      return Status::Invalid("librados::conf_read_file returned non-zero exit code.");

    if (rados->connect())
      return Status::Invalid("librados::connect returned non-zero exit code.");

    if (rados->ioctx_create(data_pool.c_str(), ioCtx))
      return Status::Invalid("librados::ioctx_create returned non-zero exit code.");

    return Status::OK();
  }

  Status Shutdown() {
    rados->shutdown();
    return Status::OK();
  }

  std::string data_pool;
  std::string user_name;
  std::string cluster_name;
  std::string ceph_config_path;
  uint64_t flags;
  std::string cls_name;

  RadosInterface* rados;
  IoCtxInterface* ioCtx;
};

class ARROW_DS_EXPORT DirectObjectAccess {
 public:
  explicit DirectObjectAccess(const std::shared_ptr<RadosCluster>& cluster)
      : cluster_(std::move(cluster)) {}

  Status Exec(const std::string& path, const std::string& fn,
              ceph::bufferlist& in,
              ceph::bufferlist& out) {
    struct stat dir_st;
    if (stat(path.c_str(), &dir_st) < 0)
      return Status::ExecutionError("stat returned non-zero exit code.");

    uint64_t inode = dir_st.st_ino;

    std::stringstream ss;
    ss << std::hex << inode;
    std::string oid(ss.str() + ".00000000");

    if (cluster_->ioCtx->exec(oid.c_str(), cluster_->cls_name.c_str(), fn.c_str(), in,
                              out)) {
      return Status::ExecutionError("librados::exec returned non-zero exit code.");
    }

    return Status::OK();
  }

 protected:
  std::shared_ptr<RadosCluster> cluster_;
};

class ARROW_DS_EXPORT RadosParquetFileFormat : public FileFormat {
 public:
  explicit RadosParquetFileFormat(
    const std::string&, const std::string&, const std::string&, const std::string&);

  explicit RadosParquetFileFormat(std::shared_ptr<DirectObjectAccess> doa)
      : doa_(std::move(doa)) {}

  std::string type_name() const override { return "rados-parquet"; }

  bool splittable() const { return true; }

  bool Equals(const FileFormat& other) const override {
    return type_name() == other.type_name();
  }

  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  Result<ScanTaskIterator> ScanFile(std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context,
                                    FileFragment* file) const override;

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options) const override {
    return Status::NotImplemented("Use the Python API");
  }

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override { return NULLPTR; }

 protected:
  std::shared_ptr<DirectObjectAccess> doa_;
};

class ARROW_DS_EXPORT RandomAccessObject : public arrow::io::RandomAccessFile {
 public:
  explicit ObjectInputFile(std::shared_ptr<RadosCluster> cluster, std::string &path)
    : cluster_(std::move(cluster)),
      path_(path) {}

  arrow::Status Init() {
    uint64_t size;
    cluster_->ioCtx->stat(path, size);
    content_length_ = size;
    return Status::OK();
  }

  arrow::Status CheckClosed() const {
    if (closed_) {
      return arrow::Status::Invalid("Operation on closed stream");
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return arrow::Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return arrow::Status::IOError("Cannot ", action, " past end of file");
    }
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) { return 0; }

  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    if (nbytes > 0) {
      ceph::bufferlist bl;
      cluster_->ioCtx->read(path, bl, nbytes, position);
      return std::make_shared<arrow::Buffer>((uint8_t*)bl.data(), bl.length());
    }
    return std::make_shared<arrow::Buffer>("");
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  arrow::Result<int64_t> GetSize() {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  arrow::Status Seek(int64_t position) {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  arrow::Status Close() {
    closed_ = true;
    return arrow::Status::OK();
  }

  bool closed() const { return closed_; }

 protected:
  std::shared_ptr<RadosCluster> cluster_;
  std::string path_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
};


}  // namespace dataset
}  // namespace arrow
