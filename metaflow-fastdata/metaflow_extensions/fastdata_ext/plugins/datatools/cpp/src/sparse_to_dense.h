#ifndef METAFLOW_DATA_SPARSE_TO_DENSE
#define METAFLOW_DATA_SPARSE_TO_DENSE

#include "c_types.h"

namespace ops {
void sparse_to_dense(ArrowArray &indices, ArrowSchema &indices_schema,
                       ArrowArray &values, ArrowSchema &values_schema,
                       int *shape, int shape_len, double null_val, void *out);
}

#endif