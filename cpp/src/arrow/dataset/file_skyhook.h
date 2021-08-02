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
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/api.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/util/iterator.h"
#include "arrow/util/macros.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"

#define SCAN_ERR_CODE 25
#define SCAN_ERR_MSG "failed to scan file fragment"

#define SCAN_REQ_DESER_ERR_CODE 26
#define SCAN_REQ_DESER_ERR_MSG "failed to deserialize scan request"

#define SCAN_RES_SER_ERR_CODE 27
#define SCAN_RES_SER_ERR_MSG "failed to serialize result table"

namespace arrow {
namespace dataset {

enum SkyhookFileType { PARQUET, IPC };

/// \addtogroup dataset-file-formats
///
/// @{

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

/// \class SkyhookFileFormat
/// \brief A ParquetFileFormat implementation that offloads the fragment
/// scan operations to the Ceph OSDs
class ARROW_DS_EXPORT SkyhookFileFormat : public ParquetFileFormat {
 public:
  SkyhookFileFormat(const std::string& file_format, const std::string& ceph_config_path,
                    const std::string& data_pool, const std::string& user_name,
                    const std::string& cluster_name, const std::string& cls_name);

  std::string type_name() const override { return "skyhook"; }

  bool splittable() const { return true; }

  bool Equals(const FileFormat& other) const override {
    return type_name() == other.type_name();
  }

  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  /// \brief Return the schema of the file fragment.
  /// \param[in] source The source of the file fragment.
  /// \return The schema of the file fragment.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  /// \brief Scan a file fragment.
  /// \param[in] options Options to pass.
  /// \param[in] file The file fragment.
  /// \return The scanned file fragment.
  Result<ScanTaskIterator> ScanFile(
      const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<FileFragment>& file) const override;

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options) const {
    return Status::NotImplemented("Use the Python API");
  }

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override { return NULLPTR; }

 protected:
  std::string file_format_;
  std::shared_ptr<CephConnCtx> ctx_;
};

/// @}

}  // namespace dataset
}  // namespace arrow
