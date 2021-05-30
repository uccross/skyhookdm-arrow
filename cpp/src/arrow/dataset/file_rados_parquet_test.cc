#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/file_rados_parquet.h"
#include "arrow/dataset/test_util.h"

namespace arrow {
namespace dataset {

TEST(TestRadosParquetFileFormat, ScanRequestSerializeDeserialize) {
    std::shared_ptr<ScanOptions> options = std::make_shared<ScanOptions>();
    options->projected_schema = arrow::schema({arrow::field("a", arrow::int64())});
    options->dataset_schema = arrow::schema({arrow::field("a", arrow::int64())});

    ceph::bufferlist bl;
    int64_t file_size = 1000000;
    SerializeScanRequest(options, file_size, bl);

    compute::Expression filter_;
    compute::Expression partition_expression_;
    std::shared_ptr<Schema> projected_schema_;
    std::shared_ptr<Schema> dataset_schema_;
    int64_t file_size_;
    DeserializeScanRequest(&filter_, &partition_expression_, &projected_schema_, &dataset_schema_, file_size_, bl);
    
    ASSERT_EQ(options->filter.Equals(filter_), 1);
    ASSERT_EQ(options->partition_expression.Equals(partition_expression_), 1);
    ASSERT_EQ(options->projected_schema->Equals(projected_schema_), 1);
    ASSERT_EQ(options->dataset_schema->Equals(dataset_schema_), 1);
}

TEST(TestRadosParquetFileFormat, SerializeTable) {
    
}

} // namespace dataset
} // namespace arrow
