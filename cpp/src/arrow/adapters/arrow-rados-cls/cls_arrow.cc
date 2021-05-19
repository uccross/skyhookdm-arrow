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
#define _FILE_OFFSET_BITS 64
#include <rados/objclass.h>
#include <memory>

#include <arrow/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/expression.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>


CLS_VER(1, 0)
CLS_NAME(arrow)

cls_handle_t h_class;
cls_method_handle_t h_scan_op;

static int scan_op(cls_method_context_t hctx, ceph::bufferlist* in,
                   ceph::bufferlist* out) {
  // the components required to construct a ParquetFragment.
  // arrow::dataset::Expression filter;
  // arrow::dataset::Expression partition_expression;
  // std::shared_ptr<arrow::Schema> projection_schema;
  std::shared_ptr<arrow::Schema> dataset_schemaq;
  int64_t file_size;

  // // // deserialize the scan request
  // if (!arrow::dataset::DeserializeScanRequestFromBufferlist(
  //          &filter, &partition_expression, &projection_schema, &dataset_schema, file_size,
  //          *in)
  //          .ok())
  //   return -1;

  // CLS_LOG(0, "deserialized scan request");

  // // scan the parquet object
  // std::shared_ptr<arrow::Table> table;
  // arrow::Status s =
  //     ScanParquetObject(hctx, filter, partition_expression, projection_schema,
  //                       dataset_schema, table, file_size);
  // if (!s.ok()) {
  //   CLS_LOG(0, "error: %s", s.message().c_str());
  //   return -1;
  // }

  // CLS_LOG(0, "performed scan");

  // // serialize the resultant table to send back to the client
  // ceph::bufferlist bl;
  // if (!arrow::dataset::SerializeTableToBufferlist(table, bl).ok()) return -1;

  // *out = bl;
  return 0;
}

void __cls_init() {
  CLS_LOG(0, "loading cls_arrow");

  cls_register("arrow", &h_class);

  cls_register_cxx_method(h_class, "scan_op", CLS_METHOD_RD, scan_op, &h_scan_op);
}
