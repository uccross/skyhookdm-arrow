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
#include "skyhook/protocol/skyhook_protocol.h"

#include <flatbuffers/flatbuffers.h>

#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/result.h"

#include "ScanRequest_generated.h"

namespace skyhook {

namespace flatbuf = org::apache::arrow::flatbuf;

arrow::Status SerializeScanRequest(ScanRequest req, ceph::bufferlist& bl) {
  auto filter_expression = arrow::compute::Serialize(req.filter_expression).ValueOrDie();
  auto partition_expression =
      arrow::compute::Serialize(req.partition_expression).ValueOrDie();
  auto projection_schema =
      arrow::ipc::SerializeSchema(*req.projection_schema).ValueOrDie();
  auto dataset_schema = arrow::ipc::SerializeSchema(*req.dataset_schema).ValueOrDie();

  flatbuffers::FlatBufferBuilder builder(1024);
  auto filter_expression_vector =
      builder.CreateVector(filter_expression->data(), filter_expression->size());
  auto partition_expression_vector =
      builder.CreateVector(partition_expression->data(), partition_expression->size());
  auto projected_schema_vector =
      builder.CreateVector(projection_schema->data(), projection_schema->size());
  auto dataset_schema_vector =
      builder.CreateVector(dataset_schema->data(), dataset_schema->size());

  auto request = flatbuf::CreateScanRequest(
      builder, req.file_size, static_cast<int>(req.file_format), filter_expression_vector,
      partition_expression_vector, dataset_schema_vector, projected_schema_vector);
  builder.Finish(request);
  uint8_t* buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  bl.append((char*)buf, size);
  return arrow::Status::OK();
}

arrow::Status DeserializeScanRequest(ScanRequest& req, ceph::bufferlist bl) {
  auto request = flatbuf::GetScanRequest((uint8_t*)bl.c_str());

  auto filter_expression_ = arrow::compute::Deserialize(
                                std::make_shared<arrow::Buffer>(
                                    request->filter()->data(), request->filter()->size()))
                                .ValueOrDie();
  req.filter_expression = filter_expression_;

  auto partition_expression_ =
      arrow::compute::Deserialize(
          std::make_shared<arrow::Buffer>(request->partition()->data(),
                                          request->partition()->size()))
          .ValueOrDie();
  req.partition_expression = partition_expression_;

  arrow::ipc::DictionaryMemo empty_memo;
  arrow::io::BufferReader projection_schema_reader(request->projection_schema()->data(),
                                                   request->projection_schema()->size());
  arrow::io::BufferReader dataset_schema_reader(request->dataset_schema()->data(),
                                                request->dataset_schema()->size());

  req.projection_schema =
      arrow::ipc::ReadSchema(&projection_schema_reader, &empty_memo).ValueOrDie();
  req.dataset_schema =
      arrow::ipc::ReadSchema(&dataset_schema_reader, &empty_memo).ValueOrDie();

  req.file_size = request->file_size();
  req.file_format = (SkyhookFileType::type)request->file_format();
  return arrow::Status::OK();
}

arrow::Status SerializeTable(std::shared_ptr<arrow::Table> table, ceph::bufferlist& bl) {
  auto buffer_output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();

  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  auto codec = arrow::Compression::LZ4_FRAME;

  ARROW_ASSIGN_OR_RAISE(options.codec,
    arrow::util::Codec::Create(codec));
  auto writer =
      arrow::ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options)
          .ValueOrDie();

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return arrow::Status::OK();
}

arrow::Status DeserializeTable(arrow::RecordBatchVector& batches, ceph::bufferlist bl,
                               bool use_threads) {
  auto buffer = std::make_shared<arrow::Buffer>((uint8_t*)bl.c_str(), bl.length());
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.use_threads = use_threads;
  auto reader =
      arrow::ipc::RecordBatchStreamReader::Open(buffer_reader, options).ValueOrDie();
  ARROW_RETURN_NOT_OK(reader->ReadAll(&batches));
  return arrow::Status::OK();
}

arrow::Status ExecuteObjectClassFn(std::shared_ptr<rados::RadosConn> connection_,
                                   const std::string& oid, const std::string& fn,
                                   ceph::bufferlist& in, ceph::bufferlist& out) {
  int e = connection_->io_ctx
              ->exec(oid.c_str(), connection_->ctx->ceph_cls_name.c_str(), fn.c_str(), in,
                     out)
              .detail().errnum();

  if (e == SCAN_ERR_CODE) return arrow::Status::Invalid(SCAN_ERR_MSG);
  if (e == SCAN_REQ_DESER_ERR_CODE) return arrow::Status::Invalid(SCAN_REQ_DESER_ERR_MSG);
  if (e == SCAN_RES_SER_ERR_CODE) return arrow::Status::Invalid(SCAN_RES_SER_ERR_MSG);
  if (e != 0) return arrow::Status::Invalid(SCAN_UNKNOWN_ERR_MSG);
  return arrow::Status::OK();
}

}  // namespace skyhook
