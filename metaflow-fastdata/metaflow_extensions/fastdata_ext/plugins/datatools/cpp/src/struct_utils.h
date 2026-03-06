#ifndef METAFLOW_DATA_STRUCT_UTILS
#define METAFLOW_DATA_STRUCT_UTILS

#include <memory>

#include "dataframe.h"

namespace ops {
std::unique_ptr<CMetaflowChunkedArray>
project_struct_list_array(struct CMetaflowChunkedArray *array,
                          const char **child_columns, int num_child_columns,
                          bool promote_single_column);
}

#endif