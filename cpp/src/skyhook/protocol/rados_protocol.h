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

#include <rados/librados.hpp>

#include "arrow/status.h"

#include "skyhook/client/file_skyhook.h"

namespace skyhook {
namespace rados {

class IoCtxInterface {
 public:
  IoCtxInterface() { ioCtx = new librados::IoCtx(); }
  ~IoCtxInterface() { delete ioCtx; }
  /// Read a RADOS object.
  arrow::Status read(const std::string& oid, ceph::bufferlist& bl, size_t len,
                   uint64_t offset);
  /// Executes a CLS function.
  arrow::Status exec(const std::string& oid, const char* cls, const char* method,
                   ceph::bufferlist& in, ceph::bufferlist& out);
  /// Execute POSIX stat on a RADOS object.
  arrow::Status stat(const std::string& oid, uint64_t* psize);
  /// Set the `librados::IoCtx` instance inside a IoCtxInterface instance.
  void setIoCtx(librados::IoCtx* ioCtx_) { *ioCtx = *ioCtx_; }
 private:
  librados::IoCtx* ioCtx;
};

class RadosInterface {
 public:
  RadosInterface() { cluster = new librados::Rados(); }
  ~RadosInterface() { delete cluster; }
  /// Initializes a cluster handle.
  arrow::Status init2(const char* const name, const char* const clustername,
                    uint64_t flags);
  /// Create an I/O context
  arrow::Status ioctx_create(const char* name, IoCtxInterface* pioctx);
  /// Read the Ceph config file.
  arrow::Status conf_read_file(const char* const path);
  /// Connect to the Ceph cluster.
  arrow::Status connect();
  /// Close connection to the Ceph cluster.
  void shutdown();

 private:
  librados::Rados* cluster;
};

/// Connect to a Ceph cluster and hold the connection
/// information for use in later stages.
class RadosConn {
 public:
  explicit RadosConn(std::shared_ptr<skyhook::RadosConnCtx> ctx)
      : ctx(std::move(ctx)),
        rados(new RadosInterface()),
        io_ctx(new IoCtxInterface()),
        connected(false) {}
  ~RadosConn();
  /// Connect to the Ceph cluster.
  arrow::Status Connect();
  /// Shutdown the connection to the Ceph
  /// cluster if already connected.
  void Shutdown();

  std::shared_ptr<skyhook::RadosConnCtx> ctx;
  RadosInterface* rados;
  IoCtxInterface* io_ctx;
  bool connected;
};

}  // namespace rados
}  // namespace skyhook
