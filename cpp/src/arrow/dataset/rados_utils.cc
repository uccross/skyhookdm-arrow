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

#include "arrow/dataset/rados_utils.h"
#include "arrow/util/compression.h"

#include <iostream>

namespace arrow {
namespace dataset {

Status WriteScanRequestToBufferList(std::shared_ptr<ScanOptions> options,
                                    int64_t file_size, ceph::bufferlist& bl) {
  ScanRequest request;

  ARROW_ASSIGN_OR_RAISE(auto filter, compute::Serialize(options->filter));
  ARROW_ASSIGN_OR_RAISE(auto partition_expression, compute::Serialize(options->partition_expression));
  ARROW_ASSIGN_OR_RAISE(auto projection_schema, ipc::SerializeSchema(*options->projected_schema));
  ARROW_ASSIGN_OR_RAISE(auto dataset_schema, ipc::SerializeSchema(*options->dataset_schema));
  
  request.set_dataset_schema((char*)dataset_schema->data());
  request.set_partition_expression((char*)partition_expression->data());
  request.set_filter_expression((char*)filter->data());
  request.set_projection_schema((char*)projection_schema->data());
  request.set_file_size(file_size);

  size_t size = request.ByteSizeLong(); 
  void *buffer = malloc(size);
  request.SerializeToArray(buffer, size);

  bl.append((char*)buffer, size);
  return Status::OK();
}

Status ReadScanRequestFromBufferList(compute::Expression* filter,
                                      compute::Expression* partition,
                                      std::shared_ptr<Schema>* projection_schema,
                                      std::shared_ptr<Schema>* dataset_schema,
                                      int64_t& file_size, ceph::bufferlist& bl) {
  ScanRequest request;
  bool done = request.ParseFromArray(bl.c_str(), bl.length());
  if (!done) {
    return Status::Invalid("Invalid Protocol Buffer message.");
  }

  ARROW_ASSIGN_OR_RAISE(*filter, compute::Deserialize(std::make_shared<Buffer>(
                                          (uint8_t*)request.filter_expression().c_str(), 
                                          request.filter_expression().length())));
  ARROW_ASSIGN_OR_RAISE(*partition, compute::Deserialize(std::make_shared<Buffer>(
                                        (uint8_t*)request.partition_expression().c_str(), 
                                        request.partition_expression().length())));
  ipc::DictionaryMemo empty_memo;
  io::BufferReader projection_schema_reader((uint8_t*)request.projection_schema().c_str(),
                                            request.projection_schema().length());
  io::BufferReader dataset_schema_reader((uint8_t*)request.dataset_schema().c_str(),
                                            request.dataset_schema().length());

  ARROW_ASSIGN_OR_RAISE(*projection_schema,
                        ipc::ReadSchema(&projection_schema_reader, &empty_memo));
  ARROW_ASSIGN_OR_RAISE(*dataset_schema,
                        ipc::ReadSchema(&dataset_schema_reader, &empty_memo));
  return Status::OK();
}

Status SerializeTableToBufferlist(std::shared_ptr<Table>& table, ceph::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  ipc::IpcWriteOptions options = ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(
      options.codec,
      util::Codec::Create(Compression::LZ4_FRAME, std::numeric_limits<int>::min()));
  ARROW_ASSIGN_OR_RAISE(
      auto writer, ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
