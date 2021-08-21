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

/// Wrap Arrow Status with a custom return code.
class RadosStatus {
 public:
  RadosStatus(arrow::Status s, int code) : s_(std::move(s)), code_(code) {}
  arrow::Status status() { return s_; }
  int code() const { return code_; }

 private:
  arrow::Status s_;
  int code_;
};

class IoCtxInterface {
 public:
  IoCtxInterface() {}

  /// \brief Read a RADOS object.
  ///
  /// \param[in] oid the object ID which to read.
  /// \param[in] bl a bufferlist to hold the contents of the read object.
  /// \param[in] len the length of data to read from an object.
  /// \param[in] offset the offset of the object to read from.
  virtual RadosStatus read(const std::string& oid, ceph::bufferlist& bl, size_t len,
                           uint64_t offset) = 0;

  /// \brief Executes a CLS function.
  ///
  /// \param[in] oid the object ID on which to execute the CLS function.
  /// \param[in] cls the name of the CLS.
  /// \param[in] method the name of the CLS function.
  /// \param[in] in a bufferlist to send data to the CLS function.
  /// \param[in] out a bufferlist to recieve data from the CLS function.
  virtual RadosStatus exec(const std::string& oid, const char* cls, const char* method,
                           ceph::bufferlist& in, ceph::bufferlist& out) = 0;

  virtual RadosStatus stat(const std::string& oid, uint64_t* psize) = 0;

 private:
  friend class RadosWrapper;
  /// \brief Set the `librados::IoCtx` instance inside a IoCtxInterface instance.
  virtual void setIoCtx(librados::IoCtx* ioCtx_) = 0;
};

class IoCtxWrapper : public IoCtxInterface {
 public:
  IoCtxWrapper() { ioCtx = new librados::IoCtx(); }
  ~IoCtxWrapper() { delete ioCtx; }
  RadosStatus read(const std::string& oid, ceph::bufferlist& bl, size_t len,
                   uint64_t offset) override;
  RadosStatus exec(const std::string& oid, const char* cls, const char* method,
                   ceph::bufferlist& in, ceph::bufferlist& out) override;
  RadosStatus stat(const std::string& oid, uint64_t* psize) override;

 private:
  void setIoCtx(librados::IoCtx* ioCtx_) override { *ioCtx = *ioCtx_; }
  librados::IoCtx* ioCtx;
};

class RadosInterface {
 public:
  RadosInterface() {}

  /// \brief Initializes a cluster handle.
  ///
  /// \param[in] name the username of the client.
  /// \param[in] clustername the name of the Ceph cluster.
  /// \param[in] flags some extra flags to pass.
  virtual RadosStatus init2(const char* const name, const char* const clustername,
                            uint64_t flags) = 0;

  /// \brief Create an I/O context
  ///
  /// \param[in] name the RADOS pool to connect to.
  /// \param[in] pioctx an instance of IoCtxInterface.
  virtual RadosStatus ioctx_create(const char* name, IoCtxInterface* pioctx) = 0;

  /// \brief Read the Ceph config file.
  ///
  /// \param[in] path the path to the config file.
  virtual RadosStatus conf_read_file(const char* const path) = 0;

  /// \brief Connect to the Ceph cluster.
  virtual RadosStatus connect() = 0;

  /// \brief Close connection to the Ceph cluster.
  virtual void shutdown() = 0;
};

class RadosWrapper : public RadosInterface {
 public:
  RadosWrapper() { cluster = new librados::Rados(); }
  ~RadosWrapper() { delete cluster; }
  RadosStatus init2(const char* const name, const char* const clustername,
                    uint64_t flags) override;
  RadosStatus ioctx_create(const char* name, IoCtxInterface* pioctx) override;
  RadosStatus conf_read_file(const char* const path) override;
  RadosStatus connect() override;
  void shutdown() override;

 private:
  librados::Rados* cluster;
};

/// Connect to a Ceph cluster and hold the connection
/// information for use in later stages.
class RadosConn {
 public:
  explicit RadosConn(std::shared_ptr<skyhook::RadosConnCtx> ctx)
      : ctx(std::move(ctx)),
        rados(new RadosWrapper()),
        io_ctx(new IoCtxWrapper()),
        connected(false) {}
  ~RadosConn();
  /// Connect to the Ceph cluster.
  arrow::Status Connect();
  /// Shutdown the connection to the Ceph
  /// cluster if already connected.
  arrow::Status Shutdown();

  std::shared_ptr<skyhook::RadosConnCtx> ctx;
  RadosInterface* rados;
  IoCtxInterface* io_ctx;
  bool connected;
};

}  // namespace rados
}  // namespace skyhook
