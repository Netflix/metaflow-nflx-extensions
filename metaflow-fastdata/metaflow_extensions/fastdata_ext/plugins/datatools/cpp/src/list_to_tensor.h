#ifndef METAFLOW_DATA_LIST_TO_TENSOR
#define METAFLOW_DATA_LIST_TO_TENSOR

#include "c_types.h"

namespace ops {
void check_list_lengths(struct ArrowArray *array, struct ArrowSchema *schema,
                        int64_t len, bool *needs_padding,
                        bool *needs_truncating);

void list_to_tensor(struct ArrowArray *array, struct ArrowSchema *schema,
                    int64_t itemsize, const char *target_type, double null_val,
                    double pad_val, void *out);

// Check if the array is a well-formed tensor, i.e. this
// is a list type (possibly nested) where the base type is a primitive
// and that the tensor is not ragged.                    
void check_tensor_format(struct ArrowArray *array,
                         struct ArrowSchema *schema, bool *is_well_formed);

// Extract the shape of the tensor.
// Assumes that the array is a well-formed tensor according to check_tensor_format.
void get_tensor_shape(struct ArrowArray *array, int64_t num_dims, int64_t *shape);

} // namespace ops

#endif