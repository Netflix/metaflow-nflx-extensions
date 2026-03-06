#include "sparse_to_dense.h"

#include "dataframe.h"
#include "util.h"

namespace {

template <typename TIndicesOffsets, typename TIndices, typename TValuesOffsets,
          typename TValues>
void convert_sparse_to_dense(int64_t list_length, const std::vector<int> &shape,
                             const ArrowArray &indices_array,
                             const ArrowArray &values_array, double null_val,
                             void *out) {
  const TIndicesOffsets *indices_offsets =
      static_cast<const TIndicesOffsets *>(indices_array.buffers[1]);
  const uint8_t *indices_bitmap =
      static_cast<const uint8_t *>(indices_array.buffers[0]);
  const bool offsets_has_null = indices_array.null_count > 0;
  const TIndices *indices =
      static_cast<const TIndices *>(indices_array.children[0]->buffers[1]);
  const TValues *values =
      static_cast<const TValues *>(values_array.children[0]->buffers[1]);
  const uint8_t *values_bitmap =
      static_cast<const uint8_t *>(values_array.children[0]->buffers[0]);
  const bool values_has_null = values_array.children[0]->null_count > 0;

  const TValues null_value = static_cast<TValues>(null_val);

  const int64_t n_dims = shape.size();
  int64_t item_size = 1;
  for (auto dim : shape) {
    item_size *= dim;
  }

  TValues *out_values = static_cast<TValues *>(out);
  TIndicesOffsets last_index = 0;
  for (int64_t i = 0; i < list_length; ++i) {
    if (offsets_has_null && ops::is_null(indices_bitmap, i)) {
      continue;
    }
    const int64_t base = i * item_size;
    TIndicesOffsets next_index = indices_offsets[i + 1];
    for (int64_t j = 0; j < (next_index - last_index) / n_dims; ++j) {
      int64_t out_index = indices[last_index + j * n_dims];
      for (int64_t k = 1; k < n_dims; ++k) {
        out_index = out_index * shape[k] + indices[last_index + j * n_dims + k];
      }
      const int64_t values_idx = last_index / n_dims + j;
      out_values[base + out_index] =
          !(values_has_null && ops::is_null(values_bitmap, values_idx))
              ? values[last_index / n_dims + j]
              : null_value;
    }
    last_index = next_index;
  }
}

template <typename TIndicesOffsets, typename TIndices, typename TValuesOffsets>
void dispatch_3(const std::vector<int> &shape, const ArrowArray &indices,
                const ArrowArray &values, const ArrowSchema &values_schema,
                double null_val, void *out) {
  std::string type(values_schema.children[0]->format);
#define DISPATCH(arrow_type, c_type)                                           \
  do                                                                           \
    if (type == #arrow_type) {                                                 \
      convert_sparse_to_dense<TIndicesOffsets, TIndices, TValuesOffsets,       \
                              c_type>(indices.length, shape, indices, values,  \
                                      null_val, out);                          \
      return;                                                                  \
    }                                                                          \
  while (0)

  DISPATCH(c, int8_t);
  DISPATCH(C, uint8_t);
  DISPATCH(s, int16_t);
  DISPATCH(S, uint16_t);
  DISPATCH(i, int32_t);
  DISPATCH(I, uint32_t);
  DISPATCH(l, int64_t);
  DISPATCH(L, uint64_t);
  DISPATCH(f, float);
  DISPATCH(g, double);
#undef DISPATCH

  throw std::runtime_error("Invalid values type: " + type);
}

template <typename TIndicesOffsets, typename TIndices>
void dispatch_2(const std::vector<int> &shape, const ArrowArray &indices,
                const ArrowArray &values, const ArrowSchema &values_schema,
                double null_val, void *out) {
  std::string type(values_schema.format);
  if (type == "+l") {
    dispatch_3<TIndicesOffsets, TIndices, int32_t>(
        shape, indices, values, values_schema, null_val, out);
  } else if (type == "+L") {
    dispatch_3<TIndicesOffsets, TIndices, int64_t>(
        shape, indices, values, values_schema, null_val, out);
  } else {
    throw std::runtime_error("Invalid values type: " + type);
  }
}

template <typename TIndicesOffsets>
void dispatch_1(const std::vector<int> &shape, const ArrowArray &indices,
                const ArrowSchema &indices_schema, const ArrowArray &values,
                const ArrowSchema &values_schema, double null_val, void *out) {
  std::string type(indices_schema.children[0]->format);
#define DISPATCH(arrow_type, c_type)                                           \
  do                                                                           \
    if (type == #arrow_type) {                                                 \
      dispatch_2<TIndicesOffsets, c_type>(shape, indices, values,              \
                                          values_schema, null_val, out);       \
      return;                                                                  \
    }                                                                          \
  while (0)

  DISPATCH(c, int8_t);
  DISPATCH(C, uint8_t);
  DISPATCH(s, int16_t);
  DISPATCH(S, uint16_t);
  DISPATCH(i, int32_t);
  DISPATCH(I, uint32_t);
  DISPATCH(l, int64_t);
  DISPATCH(L, uint64_t);
#undef DISPATCH

  throw std::runtime_error("Invalid indices type: " + type);
}

void dispatch_0(const std::vector<int> &shape, const ArrowArray &indices,
                const ArrowSchema &indices_schema, const ArrowArray &values,
                const ArrowSchema &values_schema, double null_val, void *out) {
  std::string type(indices_schema.format);
  if (type == "+l") {
    dispatch_1<int32_t>(shape, indices, indices_schema, values, values_schema,
                        null_val, out);
  } else if (type == "+L") {
    dispatch_1<int64_t>(shape, indices, indices_schema, values, values_schema,
                        null_val, out);
  } else {
    throw std::runtime_error("Invalid indices type: " + type);
  }
}
} // namespace

namespace ops {
void sparse_to_dense(ArrowArray &indices, ArrowSchema &indices_schema,
                     ArrowArray &values, ArrowSchema &values_schema, int *shape,
                     int shape_len, double null_val, void *out) {
  const std::vector<int> dims(shape, shape + shape_len);
  dispatch_0(dims, indices, indices_schema, values, values_schema, null_val,
             out);
}
} // namespace ops