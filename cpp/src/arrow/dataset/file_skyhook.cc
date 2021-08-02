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
#include "arrow/dataset/file_skyhook.h"

#include <mutex>

#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/rados_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

namespace arrow {

namespace dataset {

/// \class CephConn
/// \brief An interface to connect to a Rados cluster and hold the connection
/// information for usage in later stages.
class ARROW_DS_EXPORT CephConn {
 public:
  struct CephConnCtx {
    std::string ceph_config_path;
    std::string ceph_data_pool;
    std::string ceph_user_name;
    std::string ceph_cluster_name;
    std::string ceph_cls_name;

    CephConnCtx(const std::string& ceph_config_path, const std::string& ceph_data_pool,
                       const std::string& ceph_user_name, const std::string& ceph_cluster_name,
                       const std::string& ceph_cls_name)
        : ceph_config_path(ceph_config_path),
          ceph_data_pool(ceph_data_pool),
          ceph_user_name(ceph_user_name),
          ceph_cluster_name(ceph_cluster_name),
          ceph_cls_name(ceph_cls_name) {}
    };

  explicit CephConn(const CephConnCtx& ctx): 
        ctx(ctx),
        rados(new util::RadosWrapper()),
        io_ctx(new util::IoCtxWrapper()),
        connected(false) {}

  ~CephConn() {
    Shutdown();
  }

  /// \brief Connect to the Rados cluster.
  /// \return Status.
  Status Connect() {
    if (connected) {
      return Status::OK();
    }

    // Locks the mutex. Only one thread can pass here at a time.
    // Another thread handled the connection already.
    std::unique_lock<std::mutex> lock(mutex);
    if (connected) {
      return Status::OK();
    }
    connected = true;

    if (rados->init2(ctx.ceph_user_name.c_str(), ctx.ceph_cluster_name.c_str(), 0))
      return Status::Invalid("librados::init2 returned non-zero exit code.");

    if (rados->conf_read_file(ctx.ceph_config_path.c_str()))
      return Status::Invalid("librados::conf_read_file returned non-zero exit code.");

    if (rados->connect())
      return Status::Invalid("librados::connect returned non-zero exit code.");

    if (rados->ioctx_create(ctx.ceph_data_pool.c_str(), io_ctx))
      return Status::Invalid("librados::ioctx_create returned non-zero exit code.");

    return Status::OK();
  }

  /// \brief Shutdown the connection to the Rados cluster.
  /// \return Status.
  Status Shutdown() {
    rados->shutdown();
    return Status::OK();
  }

  CephConnCtx ctx;
  util::RadosInterface* rados;
  util::IoCtxInterface* io_ctx;
  bool connected;
  std::mutex mutex;
};

class ARROW_DS_EXPORT SkyhookDirectObjectAccess {
 public:
  explicit SkyhookDirectObjectAccess(
      const std::shared_ptr<CephConn>& connection)
      : connection_(std::move(connection)) {}

  Status Stat(const std::string& path, struct stat& st) {
    struct stat file_st;
    if (stat(path.c_str(), &file_st) < 0)
      return Status::Invalid("stat returned non-zero exit code.");
    st = file_st;
    return Status::OK();
  }

  std::string ConvertInodeToOID(uint64_t inode) {
    std::stringstream ss;
    ss << std::hex << inode;
    std::string oid(ss.str() + ".00000000");
    return oid;
  }

  Status Exec(uint64_t inode, const std::string& fn, ceph::bufferlist& in,
              ceph::bufferlist& out) {
    std::string oid = ConvertInodeToOID(inode);
    int e = connection_->io_ctx->exec(oid.c_str(), connection_->ctx.cls_name.c_str(),
                                     fn.c_str(), in, out);
    if (e == SCAN_ERR_CODE) return Status::Invalid(SCAN_ERR_MSG);
    if (e == SCAN_REQ_DESER_ERR_CODE) return Status::Invalid(SCAN_REQ_DESER_ERR_MSG);
    if (e == SCAN_RES_SER_ERR_CODE) return Status::Invalid(SCAN_RES_SER_ERR_MSG);
    return Status::OK();
  }

 protected:
  std::shared_ptr<CephConn> connection_;
};

/// \brief A ScanTask to scan a file fragment in Skyhook format.
class SkyhookScanTask : public ScanTask {
 public:
  SkyhookScanTask(std::shared_ptr<ScanOptions> options,
                  std::shared_ptr<Fragment> fragment, FileSource source,
                  std::shared_ptr<SkyhookDirectObjectAccess> doa, int fragment_format)
      : ScanTask(std::move(options), std::move(fragment)),
        source_(std::move(source)),
        doa_(std::move(doa)),
        fragment_format_(fragment_format) {}

  Result<RecordBatchIterator> Execute() override {
    struct stat st {};
    ARROW_RETURN_NOT_OK(doa_->Stat(source_.path(), st));

    ceph::bufferlist request;
    ARROW_RETURN_NOT_OK(
        util::SerializeScanRequest(options_, fragment_format_, st.st_size, request));

    ceph::bufferlist result;
    ARROW_RETURN_NOT_OK(doa_->Exec(st.st_ino, "scan_op", request, result));

    RecordBatchVector batches;
    ARROW_RETURN_NOT_OK(util::DeserializeTable(batches, result, !options_->use_threads));
    return MakeVectorIterator(batches);
  }

 protected:
  FileSource source_;
  std::shared_ptr<SkyhookDirectObjectAccess> doa_;
  int fragment_format_;
};

SkyhookFileFormat::SkyhookFileFormat(const std::string& fragment_format,
                                     const std::string& ceph_config_path,
                                     const std::string& ceph_data_pool,
                                     const std::string& ceph_user_name,
                                     const std::string& ceph_cluster_name,
                                     const std::string& ceph_cls_name) {
  CephConnCtx ctx(ceph_config_path, ceph_data_pool, ceph_user_name, ceph_cluster_name, ceph_cls_name);
  ctx_ = ctx;
  fragment_format_ = fragment_format;
}

Result<std::shared_ptr<Schema>> SkyhookFileFormat::Inspect(
    const FileSource& source) const {
  std::shared_ptr<FileFormat> format;
  if (fragment_format_ == "parquet") {
    format = std::make_shared<ParquetFileFormat>();
  } else if (fragment_format_ == "ipc") {
    format = std::make_shared<IpcFileFormat>();
  } else {
    return Status::Invalid("invalid file format");
  }
  std::shared_ptr<Schema> schema;
  ARROW_ASSIGN_OR_RAISE(schema, format->Inspect(source));
  return schema;
}

Result<ScanTaskIterator> SkyhookFileFormat::ScanFile(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& file) const {
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>(*options);
  options_->partition_expression = file->partition_expression();
  options_->dataset_schema = file->dataset_schema();

  int fragment_format = -1;
  if (fragment_format_ == "parquet")
    fragment_format = 0;
  else if (fragment_format_ == "ipc")
    fragment_format = 1;
  else
    return Status::Invalid("Unsupported file format");

  auto connection = std::make_shared<CephConn>(ctx_);
  auto doa = std::make_shared<SkyhookDirectObjectAccess>(connection);

  ScanTaskVector v{std::make_shared<SkyhookScanTask>(std::move(options_), std::move(file),
                                                     file->source(), std::move(doa),
                                                     fragment_format)};
  return MakeVectorIterator(v);
}

}  // namespace dataset
}  // namespace arrow
