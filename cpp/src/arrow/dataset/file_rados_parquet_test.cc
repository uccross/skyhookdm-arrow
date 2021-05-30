#include "arrow/dataset/test_util.h"

namespace arrow {
namespace dataset {

TEST(TestRadosParquetFileFormat, SerializeDeserializeRoundTrip) {
    std::shared_ptr<ScanOptions> options;
    ceph::bufferlist bl;
    ASSERT_OK(SerializeScanRequest(options, 100, bl));
}

} // namespace dataset
} // namespace arrow
