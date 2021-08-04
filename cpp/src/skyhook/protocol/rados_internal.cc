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

#include "skyhook/protocol/rados_internal.h"

#include <iostream>
#include <vector>

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace util {

int IoCtxWrapper::write_full(const std::string& oid, ceph::bufferlist& bl) {
  return this->ioCtx->write_full(oid, bl);
}

int IoCtxWrapper::read(const std::string& oid, ceph::bufferlist& bl, size_t len,
                       uint64_t offset) {
  return this->ioCtx->read(oid, bl, len, offset);
}

int IoCtxWrapper::exec(const std::string& oid, const char* cls, const char* method,
                       ceph::bufferlist& in, ceph::bufferlist& out) {
  return this->ioCtx->exec(oid, cls, method, in, out);
}

int IoCtxWrapper::stat(const std::string& oid, uint64_t* psize) {
  return this->ioCtx->stat(oid, psize, NULL);
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

int RadosWrapper::init2(const char* const name, const char* const clustername,
                        uint64_t flags) {
  return this->cluster->init2(name, clustername, flags);
}

int RadosWrapper::ioctx_create(const char* name, IoCtxInterface* pioctx) {
  librados::IoCtx ioCtx;
  int ret = this->cluster->ioctx_create(name, ioCtx);
  pioctx->setIoCtx(&ioCtx);
  return ret;
}

int RadosWrapper::conf_read_file(const char* const path) {
  return this->cluster->conf_read_file(path);
}

int RadosWrapper::connect() { return this->cluster->connect(); }

void RadosWrapper::shutdown() { return this->cluster->shutdown(); }

Status SerializeScanRequest(std::shared_ptr<dataset::ScanOptions>& options, int& file_size,
                            int64_t& file_format, ceph::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto filter, compute::Serialize(options->filter));
  ARROW_ASSIGN_OR_RAISE(auto partition,
                        compute::Serialize(options->partition_expression));
  ARROW_ASSIGN_OR_RAISE(auto projected_schema,
                        ipc::SerializeSchema(*options->projected_schema));
  ARROW_ASSIGN_OR_RAISE(auto dataset_schema,
                        ipc::SerializeSchema(*options->dataset_schema));

  flatbuffers::FlatBufferBuilder builder(1024);

  auto filter_vec = builder.CreateVector(filter->data(), filter->size());
  auto partition_vec = builder.CreateVector(partition->data(), partition->size());
  auto projected_schema_vec =
      builder.CreateVector(projected_schema->data(), projected_schema->size());
  auto dataset_schema_vec =
      builder.CreateVector(dataset_schema->data(), dataset_schema->size());

  auto request =
      flatbuf::CreateScanRequest(builder, file_size, file_format, filter_vec,
                                 partition_vec, dataset_schema_vec, projected_schema_vec);
  builder.Finish(request);
  uint8_t* buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  bl.append((char*)buf, size);
  return Status::OK();
}

Status DeserializeScanRequest(compute::Expression* filter, compute::Expression* partition,
                              std::shared_ptr<Schema>* projected_schema,
                              std::shared_ptr<Schema>* dataset_schema, int64_t& file_size,
                              int& file_format, ceph::bufferlist& bl) {
  auto request = flatbuf::GetScanRequest((uint8_t*)bl.c_str());

  ARROW_ASSIGN_OR_RAISE(auto filter_,
                        compute::Deserialize(std::make_shared<Buffer>(
                            request->filter()->data(), request->filter()->size())));
  *filter = filter_;

  ARROW_ASSIGN_OR_RAISE(auto partition_,
                        compute::Deserialize(std::make_shared<Buffer>(
                            request->partition()->data(), request->partition()->size())));
  *partition = partition_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader projected_schema_reader(request->projection_schema()->data(),
                                           request->projection_schema()->size());
  io::BufferReader dataset_schema_reader(request->dataset_schema()->data(),
                                         request->dataset_schema()->size());

  ARROW_ASSIGN_OR_RAISE(auto projected_schema_,
                        ipc::ReadSchema(&projected_schema_reader, &empty_memo));
  *projected_schema = projected_schema_;

  ARROW_ASSIGN_OR_RAISE(auto dataset_schema_,
                        ipc::ReadSchema(&dataset_schema_reader, &empty_memo));
  *dataset_schema = dataset_schema_;

  file_size = request->file_size();
  file_format = request->file_format();
  return Status::OK();
}

Status SerializeTable(std::shared_ptr<Table>& table, ceph::bufferlist& bl,
                      bool aggressive) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  auto options = ipc::IpcWriteOptions::Defaults();

  Compression::type codec;
  if (aggressive) {
    codec = Compression::ZSTD;
  } else {
    codec = Compression::LZ4_FRAME;
  }

  ARROW_ASSIGN_OR_RAISE(options.codec,
                        util::Codec::Create(codec, std::numeric_limits<int>::min()));
  ARROW_ASSIGN_OR_RAISE(
      auto writer, ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

Status DeserializeTable(RecordBatchVector& batches, ceph::bufferlist& bl,
                        bool use_threads) {
  auto buffer = std::make_shared<Buffer>((uint8_t*)bl.c_str(), bl.length());
  auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
  auto options = ipc::IpcReadOptions::Defaults();
  options.use_threads = use_threads;
  ARROW_ASSIGN_OR_RAISE(
      auto reader, ipc::RecordBatchStreamReader::Open(buffer_reader, options));
  ARROW_RETURN_NOT_OK(reader->ReadAll(&batches));
  return Status::OK();
}

}  // namespace util
}  // namespace arrow
