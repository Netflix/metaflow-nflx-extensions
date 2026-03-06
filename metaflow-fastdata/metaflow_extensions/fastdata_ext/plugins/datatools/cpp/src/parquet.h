#ifndef METAFLOW_DATA_READ_PARQUET
#define METAFLOW_DATA_READ_PARQUET

#include <memory>

#include "dataframe.h"

namespace ops {
std::unique_ptr<CMetaflowDataFrame>
read_parquet(const char *file_path, const char **columns,
             const int32_t *field_ids, int64_t num_columns_or_fields, bool allow_missing);

std::unique_ptr<CMetaflowDataFrame>
read_parquet_with_schema(const char *file_path, ArrowSchema* c_schema);

void write_parquet(const char *file_path, struct CMetaflowDataFrame *df,
                   const char *compression_type, int64_t offset, int64_t length,
                   int64_t num_fields, const int64_t *field_ids,
                   const char **field_paths, bool use_int96_timestamp);
} // namespace ops

#endif
