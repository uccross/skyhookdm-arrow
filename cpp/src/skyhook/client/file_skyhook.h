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
#pragma once

#include "arrow/api.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace skyhook {

/// \addtogroup dataset-file-formats
///
/// @{

/// \struct RadosConnCtx
/// \brief A struct to hold the parameters required
/// for connecting to a RADOS cluster.
struct RadosConnCtx {
  std::string ceph_config_path;
  std::string ceph_data_pool;
  std::string ceph_user_name;
  std::string ceph_cluster_name;
  std::string ceph_cls_name;

  RadosConnCtx(const std::string& ceph_config_path, const std::string& ceph_data_pool,
               const std::string& ceph_user_name, const std::string& ceph_cluster_name,
               const std::string& ceph_cls_name)
      : ceph_config_path(ceph_config_path),
        ceph_data_pool(ceph_data_pool),
        ceph_user_name(ceph_user_name),
        ceph_cluster_name(ceph_cluster_name),
        ceph_cls_name(ceph_cls_name) {}
};

/// \class SkyhookFileFormat
/// \brief A FileFormat implementation that offloads fragment
/// scan operations to the Ceph OSDs. For more details, see the
/// Skyhook paper, https://arxiv.org/pdf/2105.09894.pdf.
class SkyhookFileFormat : public arrow::dataset::ParquetFileFormat {
 public:
  static arrow::Result<std::shared_ptr<SkyhookFileFormat>> Make(std::shared_ptr<RadosConnCtx> ctx, std::string file_format);
  SkyhookFileFormat(std::shared_ptr<RadosConnCtx> ctx, std::string file_format);

  ~SkyhookFileFormat();

  std::string type_name() const override { return "skyhook"; }

  bool splittable() const { return true; }

  bool Equals(const arrow::dataset::FileFormat& other) const override {
    return type_name() == other.type_name();
  }

  arrow::Result<bool> IsSupported(
      const arrow::dataset::FileSource& source) const override {
    return true;
  }

  /// \brief Return the schema of the file fragment.
  /// \param[in] source The source of the file fragment.
  /// \return The schema of the file fragment.
  arrow::Result<std::shared_ptr<arrow::Schema>> Inspect(
      const arrow::dataset::FileSource& source) const override;

  /// \brief Scan a file fragment.
  /// \param[in] options The ScanOptions to use.
  /// \param[in] file The file fragment to scan.
  /// \return An iterator of ScanTasks.
  arrow::Result<arrow::dataset::ScanTaskIterator> ScanFile(
      const std::shared_ptr<arrow::dataset::ScanOptions>& options,
      const std::shared_ptr<arrow::dataset::FileFragment>& file) const override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  /// \brief Initialize the SkyhookFileFormat by connecting to RADOS.
  arrow::Status Init();
};

/// @}

}  // namespace skyhook
