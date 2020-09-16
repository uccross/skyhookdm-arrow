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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/mutex.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/filter.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"
#include "arrow/dataset/rados.h"

namespace arrow {
namespace dataset {

/// \brief The Object abstraction encapsulate object properties.
/// Currently it hold only the object id.
class ARROW_DS_EXPORT Object {
  public:
    Object(std::string id)
      : id_(id) {}

    std::string id() const { return id_; }

  protected:
    std::string id_;
};

/// \brief A vector of Object and an iterator over that object vector
using ObjectVector = std::vector<std::shared_ptr<Object>>;
using ObjectIterator = Iterator<std::shared_ptr<Object>>;

/// \brief Store configuration for connecting to a RADOS backend and 
/// the CLS library and functions to invoke. Also, holds the cluster 
// and io_ctx context.
class ARROW_DS_EXPORT RadosOptions {
  public:
    RadosOptions(std::string pool_name,
                 std::string ceph_config_path = "/etc/ceph/ceph.conf",
                 std::string user_name = "client.admin", 
                 std::string cluster_name = "ceph",
                 uint64_t flags = 0,
                 std::string cls_name = "arrow", 
                 std::string cls_method = "read")
        : pool_name_(pool_name),
          user_name_(user_name), 
          cluster_name_(cluster_name), 
          ceph_config_path_(ceph_config_path),
          flags_(flags),
          cls_name_(cls_name),
          cls_method_(cls_method) {}

    std::string pool_name_;
    std::string user_name_;
    std::string cluster_name_;
    std::string ceph_config_path_;
    uint64_t flags_;
    std::string cls_name_;
    std::string cls_method_;

    /// \brief Set the Rados interface
    Status SetRadosInterface(RadosInterface *rados_interface) { 
      rados_interface_ = rados_interface;
      return Status::OK();
    }

    /// \brief Set the IoCtx interface
    Status SetIoCtxInterface(IoCtxInterface *io_ctx_interface) {
      io_ctx_interface_ = io_ctx_interface;
      return Status::OK();
    }

    /// \brief Get the Rados interface
    RadosInterface* GetRadosInterface() { 
      return rados_interface_;
    }

    /// \brief Get the IoCtx interface
    IoCtxInterface* GetIoCtxInterface() {
      return io_ctx_interface_;
    }

  protected:
    RadosInterface *rados_interface_ = new RadosWrapper();
    IoCtxInterface *io_ctx_interface_ = new IoCtxWrapper();
};

/// \brief A Fragment that maps to an object in the backend.
/// A RadosFragment stores a vector of RecordBatches in the form
/// of an arrow Table.
class ARROW_DS_EXPORT RadosFragment : public Fragment {
  public:
    RadosFragment(std::shared_ptr<Schema> schema, 
                  std::shared_ptr<Object> object,
                  std::shared_ptr<RadosOptions> rados_options)
        : Fragment(scalar(true), std::move(schema)), 
          object_(std::move(object)), 
          rados_options_(std::move(rados_options)) {}

    Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                  std::shared_ptr<ScanContext> context) override;

    std::string type_name() const { return "rados"; }

    bool splittable() const override { return false; }

  protected:
    Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;
    std::shared_ptr<Object> object_;
    std::shared_ptr<RadosOptions> rados_options_;
};

/// \brief A RadosDataset wraps a vector of Objects and generates 
/// RadosFragments out of them.
class ARROW_DS_EXPORT RadosDataset : public Dataset {
 public:
  class ObjectGenerator {
    public:
      virtual ~ObjectGenerator() = default;
      virtual ObjectIterator Get() const = 0;
  };

  RadosDataset(std::shared_ptr<Schema> schema,
               std::shared_ptr<ObjectGenerator> get_objects,
               std::shared_ptr<RadosOptions> rados_options)
      : Dataset(std::move(schema)), get_objects_(std::move(get_objects)), rados_options_(std::move(rados_options)) {}

  RadosDataset(std::shared_ptr<Schema> schema, 
               ObjectVector objects,
               std::shared_ptr<RadosOptions> rados_options);

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  const std::shared_ptr<RadosOptions>& rados_options() const { return rados_options_; }

  std::string type_name() const { return "rados"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;
  
  /// \brief Connect to the Rados cluster
  Status Connect();

  /// \brief Shutdown the connection to the Rados cluster
  Status Shutdown();

 protected:
  /// \brief Generates fragments from the dataset
  FragmentIterator GetFragmentsImpl(std::shared_ptr<Expression> predicate = scalar(true)) override;
  std::shared_ptr<ObjectGenerator> get_objects_;
  std::shared_ptr<RadosOptions> rados_options_;
};

class ARROW_DS_EXPORT RadosScanTask : public ScanTask {
  public: 
    RadosScanTask(std::shared_ptr<ScanOptions> options, 
                  std::shared_ptr<ScanContext> context,
                  std::shared_ptr<Object> object,
                  std::shared_ptr<RadosOptions> rados_options)
        : ScanTask(std::move(options), std::move(context)), 
          object_(std::move(object)), 
          rados_options_(rados_options) {}

    Result<RecordBatchIterator> Execute() override;

  protected:
    std::shared_ptr<Object> object_;
    std::shared_ptr<RadosOptions> rados_options_;
};

}  // namespace dataset
}  // namespace arrow
