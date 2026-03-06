#ifndef METAFLOW_DATA_COLUMN
#define METAFLOW_DATA_COLUMN

#include "dataframe.h"

namespace ops {
CMetaflowDataFrame *add_null_column(CMetaflowDataFrame &df, const char *name,
                                    const char *arrow_type,
                                    const CMetaflowChunkedArray *column);

bool is_same_value(struct CMetaflowChunkedArray *array,
                   const int64_t * value_int,
                   const char * value_bytes);
} // namespace ops

#endif
