#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/file_rados_parquet.h"
#include "arrow/dataset/test_util.h"

namespace arrow {
namespace dataset {

TEST(TestRadosParquetFileFormat, SerializeDeserializeRoundTrip) {
    std::shared_ptr<ScanOptions> options;
    ceph::bufferlist bl;
    int64_t file_size = 1000000;
    RETURN_NOT_OK(SerializeScanRequest(options, file_size, bl));

    compute::Expression filter_;
    compute::Expression partition_expression_;
    std::shared_ptr<Schema> projected_schema;
    std::shared_ptr<Schema> dataset_schema_;
    int64_t file_size_;
    RETURN_NOT_OK(DeserializeScanRequest(filter_, partition_expression_, projected_schema, dataset_schema_, file_size_, bl);
}

} // namespace dataset
} // namespace arrow
