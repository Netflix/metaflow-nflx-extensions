#include "list_to_tensor.h"

#include <stdexcept>
#include <string>
#include <vector>

#include "util.h"

// primitive types in c_arrow format
// since tensors can only have a primitive base type
static constexpr const char *c_arrow_primitive_formats[] = {
    "n", "b", "c", "C", "s", "S", "i", "I", "l", "L", "e", "f", "g"
};
static constexpr const size_t num_c_arrow_primitives = sizeof(c_arrow_primitive_formats) / sizeof(c_arrow_primitive_formats[0]);

namespace {
// Wrapper for value array to handle bit-backed bool values
template <typename TValue> class ValueReader {
public:
  ValueReader(const void *buffer, size_t offset, size_t /* length */)
      : buffer_(static_cast<const TValue *>(buffer) + offset) {}

  TValue operator[](size_t i) const { return buffer_[i]; }

  const TValue *operator+(size_t i) const { return buffer_ + i; }

private:
  const TValue *buffer_;
};

// Handle bool values separately as they are bit-packed by arrow.
template <> class ValueReader<bool> {
public:
  ValueReader(const void *buffer, size_t offset, size_t length)
      : buffer_(length) {
    const uint8_t *bool_buffer = static_cast<const uint8_t *>(buffer);
    for (size_t i = offset; i < offset + length; ++i) {
      const uint8_t bit_i = bool_buffer[i / 8] & (1 << (i % 8));
      buffer_[i] = (bit_i != 0);
    }
  }

  bool operator[](size_t i) const { return buffer_[i]; }

  const bool *operator+(size_t i) const {
    return reinterpret_cast<const bool *>(buffer_.data()) + i;
  }

private:
  // cannot use std::vector<bool> since that may also be bitpacked
  std::vector<uint8_t> buffer_;
};

template <typename TIndex, typename TSrcValue, typename TDstValue>
void array_to_tensor(const ArrowArray &array, int64_t itemsize, double null_val,
                     double pad_val, void *out_void) {
  const TDstValue null_value = static_cast<TDstValue>(null_val);
  const TDstValue pad_value = static_cast<TDstValue>(pad_val);

  const uint8_t *list_bitmap =
      static_cast<const uint8_t *>(array.buffers[0]) + array.offset;
  const TIndex *offsets =
      static_cast<const TIndex *>(array.buffers[1]) + array.offset;
  const uint8_t *values_bitmap =
      static_cast<const uint8_t *>(array.children[0]->buffers[0]) +
      array.children[0]->offset;
  const ValueReader<TSrcValue> values(array.children[0]->buffers[1],
                                      array.children[0]->offset,
                                      array.children[0]->length);

  TDstValue *out = static_cast<TDstValue *>(out_void);

  const bool list_has_nulls = array.null_count > 0;
  const bool values_has_null = array.children[0]->null_count > 0;

  const int64_t list_length = array.length;
  int64_t out_idx = 0;

  for (int64_t row = 0; row < list_length; ++row) {
    TDstValue *out_ptr = out + out_idx;
    if (!list_has_nulls && !values_has_null) {
      // Fast path: Check if a range of lists all have length itemsize
      const int64_t start_row = row;
      int64_t offset = offsets[start_row];
      int64_t end_row = row;
      while (end_row < list_length &&
             offsets[end_row + 1] == offset + itemsize) {
        end_row += 1;
        offset += itemsize;
      }
      if (end_row > start_row) {
        std::copy(values + offsets[start_row], values + offsets[end_row],
                  out_ptr);
        row = end_row - 1;
        out_idx += offsets[end_row] - offsets[start_row];
        continue;
      }
    }

    const bool list_is_null = list_has_nulls && ops::is_null(list_bitmap, row);

    if (!list_is_null) {
      const int64_t row_start = offsets[row];
      const int64_t row_end =
          std::min<int64_t>(offsets[row + 1], row_start + itemsize);
      if (!values_has_null) {
        // No values are null: use simple copy
        std::copy(values + row_start, values + row_end, out + out_idx);
      } else {
        // Some values are null: Check every element before copying
        for (int64_t i = row_start; i < row_end; ++i) {
          out[out_idx + i - row_start] =
              ops::is_null(values_bitmap, i) ? null_value : values[i];
        }
      }

      if (row_end < row_start + itemsize) {
        // List is shorter than output row size: pad row
        std::fill(out_ptr + row_end - row_start, out_ptr + itemsize, pad_value);
      }
    } else {
      // list is null: convert to empty and pad
      std::fill(out_ptr, out_ptr + itemsize, pad_value);
    }

    out_idx += itemsize;
  }
}

template <typename TIndex, typename TSrcValue>
void dispatch_by_dst(const ArrowArray &array, int64_t itemsize,
                     const std::string &target_type, double null_val,
                     double pad_val, void *out) {
#define DISPATCH(arrow_type, c_type)                                           \
  do {                                                                         \
    if (target_type == #arrow_type) {                                          \
      array_to_tensor<TIndex, TSrcValue, c_type>(array, itemsize, null_val,    \
                                                 pad_val, out);                \
    }                                                                          \
  } while (0)

  DISPATCH(b, bool);
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

  throw std::runtime_error("Invalid target type: " + target_type);
}

template <typename TIndex>
void dispatch_by_src(const ArrowArray &array, const ArrowSchema &schema,
                     int64_t itemsize, const std::string &target_type,
                     double null_val, double pad_val, void *out) {
  std::string child_type(schema.children[0]->format);

#define DISPATCH(arrow_type, c_type)                                           \
  do {                                                                         \
    if (child_type == #arrow_type) {                                           \
      dispatch_by_dst<TIndex, c_type>(array, itemsize, target_type, null_val,  \
                                      pad_val, out);                           \
      return;                                                                  \
    }                                                                          \
  } while (0)

  DISPATCH(b, bool);
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

  throw std::runtime_error("Invalid inner type: " + child_type);
}

template <typename TIndex>
void check_offsets(const ArrowArray &array, int64_t len, bool *needs_padding,
                   bool *needs_truncating) {
  const TIndex *offsets =
      static_cast<const TIndex *>(array.buffers[1]) + array.offset;
  for (int64_t row = 0; row < array.length; ++row) {
    const int64_t row_len = offsets[row + 1] - offsets[row];
    if (row_len < len) {
      *needs_padding = true;
    } else if (row_len > len) {
      *needs_truncating = true;
    }

    if (*needs_padding && *needs_truncating) {
      return;
    }
  }
}

template <typename TIndex>
void check_tensor_offsets(const ArrowArray &array, const ArrowSchema &schema, 
                   bool *is_well_formed) {
  *is_well_formed = true;

  // check that this is a list type
  const std::string type(schema.format);
  if (type != "+l" && type != "+L") {
    // make sure the base data type is a primitive type before returning
    for (size_t i = 0; i < num_c_arrow_primitives; ++i) {
        if (strcmp(type.c_str(), c_arrow_primitive_formats[i]) == 0) {
            return;
        }
    }
    *is_well_formed = false;
    return;
  }

  // check the offsets at this level to make sure they are the same
  if (array.length < 2) {
    return;
  }

  const TIndex *offsets =
      static_cast<const TIndex *>(array.buffers[1]) + array.offset;
  int64_t target_len = offsets[1] - offsets[0];
  
  for (int64_t row = 1; row < array.length; ++row) {
    const int64_t row_len = offsets[row + 1] - offsets[row];
    if (row_len != target_len) {
      *is_well_formed = false;
      return;
    }
  }
  
  // recursive call setup
  const int64_t n_children = array.n_children;
  if (n_children == 0) {
    return;
  }
  // there should only be one child in a list type so we just have to check that
  check_tensor_offsets<TIndex>(*array.children[0], *schema.children[0], is_well_formed);
}
} // namespace

namespace ops {
void check_list_lengths(struct ArrowArray *array, struct ArrowSchema *schema,
                        int64_t len, bool *needs_padding,
                        bool *needs_truncating) {
  *needs_padding = false;
  *needs_truncating = false;
  if (array->null_count > 0) {
    // Null lists are treated as empty lists
    *needs_padding = true;
  }

  const std::string list_type(schema->format);
  if (list_type == "+l") {
    check_offsets<int32_t>(*array, len, needs_padding, needs_truncating);
  } else if (list_type == "+L") {
    check_offsets<int64_t>(*array, len, needs_padding, needs_truncating);
  } else {
    throw std::runtime_error("Invalid list type: " + list_type);
  }
}

void list_to_tensor(struct ArrowArray *array, struct ArrowSchema *schema,
                    int64_t itemsize, const char *c_target_type,
                    double null_val, double pad_val, void *out) {
  const std::string list_type(schema->format);
  const std::string target_type(c_target_type);
  if (list_type == "+l") {
    dispatch_by_src<int32_t>(*array, *schema, itemsize, target_type, null_val,
                             pad_val, out);
  } else if (list_type == "+L") {
    dispatch_by_src<int64_t>(*array, *schema, itemsize, target_type, null_val,
                             pad_val, out);
  } else {
    throw std::runtime_error("Invalid list type: " + list_type);
  }
}

// Check if the array is a well-formed tensor, i.e. this
// is a list type (possibly nested) where the base type is a primitive
// and that the tensor is not ragged.
void check_tensor_format(struct ArrowArray *array,
                        struct ArrowSchema *schema, 
                        bool *is_well_formed) {
  if (array == nullptr)
    throw std::runtime_error("The column must be non-null");

  if (schema == nullptr)
    throw std::runtime_error("The schema must be non-null");

  if (is_well_formed == nullptr)
    throw std::runtime_error("The is_well_formed must be non-null");

  const std::string list_type(schema->format);
  if (list_type == "+l") {
    check_tensor_offsets<int32_t>(*array, *schema, is_well_formed);
  } else if (list_type == "+L") {
    check_tensor_offsets<int64_t>(*array, *schema, is_well_formed);
  } else {
    throw std::runtime_error("Invalid type for tensor: " + list_type);
  }

}

// Extract the shape of the tensor.
// Assumes that the array is a well-formed tensor according to check_tensor_format.
void get_tensor_shape(struct ArrowArray *array, int64_t num_dims, int64_t *shape) {
  if (array == nullptr)
    throw std::runtime_error("The column must be non-null");

  for (int64_t idx = 0; idx < num_dims; ++idx) {
    shape[idx] = 0;
  }

  ArrowArray *curr_array = array;
  int64_t curr_num_elems = 1;

  for (int64_t idx = 0; idx < num_dims; ++idx) {
    // at every level, we have a flattened array of the list elements
    // new dimension = new flattened array length / num elements in the previous array
    int64_t new_dim = curr_array->length / curr_num_elems;
    curr_num_elems *= new_dim;
    shape[idx] = new_dim;

    if (curr_array->n_children == 0 || curr_array->children[0] == nullptr) {
      break;
    }
    curr_array = curr_array->children[0];
  }
}
} // namespace ops