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
#include "arrow/dataset/file_rados_parquet.h"

#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

namespace arrow {
namespace dataset {

class RadosParquetScanTask : public ScanTask {
 public:
  RadosParquetScanTask(std::shared_ptr<ScanOptions> options,
                       std::shared_ptr<Fragment> fragment,
                       std::shared_ptr<DirectObjectAccess> doa,
                       uint64_t inode,
                       ceph::bufferlist request)
      : ScanTask(std::move(options), std::move(fragment)),
        doa_(std::move(doa)),
        inode_(std::move(inode)),
        request_(std::move(request)) {}

  Result<RecordBatchIterator> Execute() override {
    ceph::bufferlist out;

    Status s = doa_->Exec(inode_, "scan_op", request_, out);
    if (!s.ok()) {
      return Status::ExecutionError(s.message());
    }

    RecordBatchVector batches;
    auto buffer = std::make_shared<Buffer>((uint8_t*)out.c_str(), out.length());
    auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
    auto options = ipc::IpcReadOptions::Defaults();
    options.use_threads = false;
    ARROW_ASSIGN_OR_RAISE(auto rb_reader, arrow::ipc::RecordBatchStreamReader::Open(
                                              buffer_reader, options));
    RecordBatchVector rbatches;
    rb_reader->ReadAll(&rbatches);
    return MakeVectorIterator(rbatches);
  }

 protected:
  std::shared_ptr<DirectObjectAccess> doa_;
  uint64_t inode_;
  ceph::bufferlist request_;
};

RadosParquetFileFormat::RadosParquetFileFormat(const std::string& ceph_config_path,
                                               const std::string& data_pool,
                                               const std::string& user_name,
                                               const std::string& cluster_name) {
  arrow::dataset::RadosCluster::RadosConnectionCtx ctx;
  ctx.ceph_config_path = "/etc/ceph/ceph.conf";
  ctx.data_pool = "cephfs_data";
  ctx.user_name = "client.admin";
  ctx.cluster_name = "ceph";
  ctx.cls_name = "arrow";
  auto cluster = std::make_shared<RadosCluster>(ctx);
  cluster->Connect();
  auto doa = std::make_shared<arrow::dataset::DirectObjectAccess>(cluster);
  doa_ = doa;
}

Result<std::shared_ptr<Schema>> RadosParquetFileFormat::Inspect(
    const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, GetReader(source));
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(reader->GetSchema(&schema));
  return schema;
}

Result<ScanTaskIterator> RadosParquetFileFormat::ScanFile(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& file) const {
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>(*options);
  options_->partition_expression = file->partition_expression();
  options_->dataset_schema = file->dataset_schema();

  struct stat st {};
  Status s = doa_->Stat(file->source().path(), st);
  if (!s.ok()) {
    return Status::Invalid(s.message());
  }
  ARROW_LOG(INFO) << "Starting Scan\n";
  if (cached_scan_request_ != NULL) {
    ARROW_LOG(INFO) << "Already cached !\n";
    scan_request = *cached_scan_request_;
  } else {
    ARROW_LOG(INFO) << "Not cached. Caching !\n";
    cached_scan_request_ = new ceph::bufferlist;
    ARROW_RETURN_NOT_OK(SerializeScanRequestToBufferlist(options_, st.st_size, *cached_scan_request_));
    ARROW_LOG(INFO) << "Serialized\n";

    ARROW_LOG(INFO) << "Cached successfully\n";

  }
  ARROW_LOG(INFO) << "Launching ScanTasks\n";

  ScanTaskVector v{std::make_shared<RadosParquetScanTask>(
      std::move(options_), std::move(file), std::move(doa_), st.st_ino, scan_request)};
  return MakeVectorIterator(v);
}

}  // namespace dataset
}  // namespace arrow
