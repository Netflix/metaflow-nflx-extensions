#ifndef METAFLOW_DATA_SELECT_ROWS
#define METAFLOW_DATA_SELECT_ROWS

#include <memory>

#include "dataframe.h"

namespace ops {
std::unique_ptr<CMetaflowDataFrame>
select_rows(struct CMetaflowDataFrame *df,
            struct CMetaflowChunkedArray *indicator, int null_conversion_policy,
            unsigned int threads, unsigned int min_run_threshold);
}

#endif