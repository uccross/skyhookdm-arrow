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
#include "skyhook/protocol/rados_protocol.h"

#include <iostream>
#include <vector>

namespace skyhook {
namespace rados {

RadosStatus GetStatusFromReturnCode(int code, std::string msg) {
  if (code) return RadosStatus(arrow::Status::Invalid(msg), code);
  return RadosStatus(arrow::Status::OK(), code);
}

RadosStatus IoCtxWrapper::write_full(const std::string& oid, ceph::bufferlist& bl) {
  return GetStatusFromReturnCode(this->ioCtx->write_full(oid, bl),
                                 "ioctx->write_full failed.");
}

RadosStatus IoCtxWrapper::read(const std::string& oid, ceph::bufferlist& bl, size_t len,
                               uint64_t offset) {
  return GetStatusFromReturnCode(this->ioCtx->read(oid, bl, len, offset),
                                 "ioctx->read failed.");
}

RadosStatus IoCtxWrapper::exec(const std::string& oid, const char* cls,
                               const char* method, ceph::bufferlist& in,
                               ceph::bufferlist& out) {
  return GetStatusFromReturnCode(this->ioCtx->exec(oid, cls, method, in, out),
                                 "ioctx->exec failed.");
}

RadosStatus IoCtxWrapper::stat(const std::string& oid, uint64_t* psize) {
  return GetStatusFromReturnCode(this->ioCtx->stat(oid, psize, NULL),
                                 "ioctx->stat failed.");
}

std::vector<std::string> IoCtxWrapper::list() {
  std::vector<std::string> oids;
  librados::NObjectIterator begin = this->ioCtx->nobjects_begin();
  librados::NObjectIterator end = this->ioCtx->nobjects_end();
  for (; begin != end; begin++) {
    oids.push_back(begin->get_oid());
  }
  return oids;
}

RadosStatus RadosWrapper::init2(const char* const name, const char* const clustername,
                                uint64_t flags) {
  return GetStatusFromReturnCode(this->cluster->init2(name, clustername, flags),
                                 "rados->init failed.");
}

RadosStatus RadosWrapper::ioctx_create(const char* name, IoCtxInterface* pioctx) {
  librados::IoCtx ioCtx;
  int ret = this->cluster->ioctx_create(name, ioCtx);
  pioctx->setIoCtx(&ioCtx);
  return GetStatusFromReturnCode(ret, "rados->ioctx_create failed.");
}

RadosStatus RadosWrapper::conf_read_file(const char* const path) {
  return GetStatusFromReturnCode(this->cluster->conf_read_file(path),
                                 "rados->conf_read_file failed.");
}

RadosStatus RadosWrapper::connect() {
  return GetStatusFromReturnCode(this->cluster->connect(), "rados->connect failed.");
}

void RadosWrapper::shutdown() { this->cluster->shutdown(); }

RadosConn::~RadosConn() { Shutdown(); }

arrow::Status RadosConn::Connect() {
  if (connected) {
    return arrow::Status::OK();
  }

  ARROW_RETURN_NOT_OK(
      rados->init2(ctx->ceph_user_name.c_str(), ctx->ceph_cluster_name.c_str(), 0)
          .status());
  ARROW_RETURN_NOT_OK(rados->conf_read_file(ctx->ceph_config_path.c_str()).status());
  ARROW_RETURN_NOT_OK(rados->connect().status());
  ARROW_RETURN_NOT_OK(rados->ioctx_create(ctx->ceph_data_pool.c_str(), io_ctx).status());
  return arrow::Status::OK();
}

arrow::Status RadosConn::Shutdown() {
  if (connected) {
    rados->shutdown();
    connected = false;
  }
  return arrow::Status::OK();
}

}  // namespace rados
}  // namespace skyhook
