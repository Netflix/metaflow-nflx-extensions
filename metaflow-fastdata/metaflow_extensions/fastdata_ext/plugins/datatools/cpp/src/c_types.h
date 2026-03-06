#ifndef METAFLOW_DATA_C_TYPES
#define METAFLOW_DATA_C_TYPES

#include <stdint.h>

#include "arrow/c/abi.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowChunkedArray {
  int64_t n_chunks;
  struct ArrowArray** chunks;
};

// Forward declare wrapper types
struct CMetaflowDataFrame;
struct CMetaflowChunkedArray;
struct CMetaflowArray;

#ifdef __cplusplus
}
#endif

#endif