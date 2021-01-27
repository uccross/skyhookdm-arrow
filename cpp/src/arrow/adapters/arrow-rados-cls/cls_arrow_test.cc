
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

#include <iostream>
#include <random>
#include <rados/objclass.h>
#include <rados/librados.hpp>
#include "arrow/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/util/iterator.h"
#include "arrow/adapters/arrow-rados-cls/cls_arrow_test_utils.h"
#include "gtest/gtest.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

using arrow::dataset::string_literals::operator"" _;

std::shared_ptr<arrow::Table> CreateTestTable() {
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  arrow::Int32Builder id_builder(pool);
  arrow::DoubleBuilder cost_builder(pool);
  arrow::ListBuilder components_builder(pool,
                                        std::make_shared<arrow::DoubleBuilder>(pool));

  arrow::DoubleBuilder& cost_components_builder =
      *(static_cast<arrow::DoubleBuilder*>(components_builder.value_builder()));

  for (int i = 0; i < 10; ++i) {
    id_builder.Append(i);
    cost_builder.Append(i + 1.0);
    components_builder.Append();
    std::vector<double> nums;
    nums.push_back(i + 1.0);
    nums.push_back(i + 2.0);
    nums.push_back(i + 3.0);
    cost_components_builder.AppendValues(nums.data(), nums.size());
  }

  std::shared_ptr<arrow::Int32Array> id_array;
  id_builder.Finish(&id_array);
  std::shared_ptr<arrow::DoubleArray> cost_array;
  cost_builder.Finish(&cost_array);

  std::shared_ptr<arrow::ListArray> cost_components_array;
  components_builder.Finish(&cost_components_array);

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int32()), arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});
}

arrow::RecordBatchVector CreateTestRecordBatches() {
  auto table = CreateTestTable();
  arrow::TableBatchReader table_reader(*table);
  arrow::RecordBatchVector batches;
  table_reader.ReadAll(&batches);
  return batches;
}

std::shared_ptr<arrow::dataset::RadosCluster> CreateTestClusterHandle() {
  auto cluster = std::make_shared<arrow::dataset::RadosCluster>("cephfs_data",
                                                                "/etc/ceph/ceph.conf");
  cluster->Connect();
  return cluster;
}

arrow::dataset::RadosDatasetFactoryOptions CreateTestRadosFactoryOptions() {
  auto cluster = CreateTestClusterHandle();
  arrow::dataset::RadosDatasetFactoryOptions factory_options;
  factory_options.ceph_config_path = cluster->ceph_config_path;
  factory_options.cls_name = cluster->cls_name;
  factory_options.cluster_name = cluster->cluster_name;
  factory_options.flags = cluster->flags;
  factory_options.pool_name = cluster->pool_name;
  factory_options.user_name = cluster->user_name;
  return factory_options;
}

std::shared_ptr<arrow::dataset::RadosFileSystem> CreateTestRadosFileSystem() {
  auto cluster = CreateTestClusterHandle();
  auto fs = std::make_shared<arrow::dataset::RadosFileSystem>();
  arrow::Status s = fs->Init(cluster);
  if (!s.ok()) std::cout << "Init() failed.\n";
  return fs;
}

double RandDouble(double min, double max) {
  return min + ((double)rand() / RAND_MAX) * (max - min);
}

int32_t RandInt32(int32_t min, int32_t max) {
  return min + (rand() % static_cast<int>(max - min + 1));
}

std::shared_ptr<arrow::Table> CreatePartitionedTable() {
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  arrow::Int32Builder sales_builder(pool);
  for (int i = 0; i < 10; i++) {
    sales_builder.Append(RandInt32(800, 1000));
  }
  std::shared_ptr<arrow::Int32Array> sales_array;
  sales_builder.Finish(&sales_array);

  arrow::DoubleBuilder price_builder(pool);
  for (int i = 0; i < 10; i++) {
    price_builder.Append(RandDouble(38999.56, 99899.23));
  }
  std::shared_ptr<arrow::DoubleArray> price_array;
  price_builder.Finish(&price_array);

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("sales", arrow::int32()), 
      arrow::field("price", arrow::float64())
  };
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, {sales_array, price_array});
}

// TEST(TestClsSDK, EndToEndWithoutPartitionPruning) {
//   auto table = CreateTestTable();
//   auto batches = CreateTestRecordBatches();
//   auto fs = CreateTestRadosFileSystem();
//   auto foptions = CreateTestRadosFactoryOptions();
//   foptions.partition_base_dir = "/mydir";

//   auto writer = std::make_shared<arrow::dataset::SplittedParquetWriter>(fs);
//   for (int i = 0; i < 8; i++) {
//     std::string path =  "/mydir/mysubdir/abc." + std::to_string(i) + ".parquet";
//     writer->WriteTable(table, path);
//   }
  
//   auto filter = ("id"_ > int32_t(5)).Copy();
//   auto projection = std::vector<std::string>{"id", "cost", "cost_components"};

//   arrow::dataset::FinishOptions finish_options;
//   auto factory = arrow::dataset::RadosDatasetFactory::Make(fs, foptions).ValueOrDie();
//   auto dataset = factory->Finish(finish_options).ValueOrDie();
//   auto scanner_builder = dataset->NewScan().ValueOrDie();
//   scanner_builder->Filter(filter);
//   scanner_builder->Project(projection);
//   auto scanner = scanner_builder->Finish().ValueOrDie();
//   auto result_table = scanner->ToTable().ValueOrDie();

//   arrow::RecordBatchVector batches_;
//   for (auto batch : batches) {
//     for (int i = 0; i < 8; i++) {
//       batches_.push_back(batch);
//     }
//   }
//   auto dataset_ = std::make_shared<arrow::dataset::InMemoryDataset>(batches_[0]->schema(), batches_);
//   auto scanner_builder_ = dataset_->NewScan().ValueOrDie();
//   scanner_builder_->Filter(filter);
//   scanner_builder_->Project(projection);
//   auto scanner_ = scanner_builder_->Finish().ValueOrDie();
//   auto result_table_ = scanner_->ToTable().ValueOrDie();

//   ASSERT_EQ(result_table->Equals(*result_table_), 1);
// }

TEST(TestClsSDK, EndToEndWithPartitionPruning) {
  auto fs = CreateTestRadosFileSystem();
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.partition_base_dir = "/tesla";

  auto writer = std::make_shared<arrow::dataset::SplittedParquetWriter>(fs);
  writer->WriteTable(CreatePartitionedTable(), "/tesla/year=2018/18UK.parquet");
  writer->WriteTable(CreatePartitionedTable(), "/tesla/year=2018/18US.parquet");
  writer->WriteTable(CreatePartitionedTable(), "/tesla/year=2019/19UK.parquet");
  writer->WriteTable(CreatePartitionedTable(), "/tesla/year=2019/19US.parquet");
  writer->WriteTable(CreatePartitionedTable(), "/tesla/year=2020/20UK.parquet");
  writer->WriteTable(CreatePartitionedTable(), "/tesla/year=2020/20US.parquet");

  factory_options.partitioning = std::make_shared<arrow::dataset::HivePartitioning>(
      arrow::schema({arrow::field("year", arrow::int32())}));

  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(fs, factory_options).ValueOrDie();
  auto ds = factory->Finish(finish_options).ValueOrDie();

  auto builder = ds->NewScan().ValueOrDie();
  auto projection = std::vector<std::string>{"year", "price"};
  // auto filter = ("sales"_ > int32_t(900) && "price"_ > double(90000.0f) && "year"_ == 2018).Copy();
  auto filter = ("sales"_ > int32_t(700) && "price"_ > double(80000.0f)).Copy();

  builder->Project(projection);
  builder->Filter(filter);
  auto scanner = builder->Finish().ValueOrDie();

  auto table = scanner->ToTable().ValueOrDie();
  std::cout << table->ToString() << "\n";
  std::cout << table->num_rows() << "\n";
}
