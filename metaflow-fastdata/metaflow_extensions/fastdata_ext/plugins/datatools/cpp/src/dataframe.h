#ifndef METAFLOW_DATA_NEW_DATAFRAME
#define METAFLOW_DATA_NEW_DATAFRAME

#include <memory>

#include "arrow/array.h"
#include "arrow/table.h"

#include "c_types.h"

struct CMetaflowArray {
  CMetaflowArray(std::shared_ptr<arrow::Array> array,
                 std::shared_ptr<arrow::Field> schema)
      : array(std::move(array)), schema(std::move(schema)) {}

  std::unique_ptr<ArrowArray> c_array();
  std::unique_ptr<ArrowSchema> c_schema();

  std::shared_ptr<arrow::Array> array;
  std::shared_ptr<arrow::Field> schema;
};

struct CMetaflowChunkedArray {
  CMetaflowChunkedArray(std::shared_ptr<arrow::ChunkedArray> chunked_array,
                        std::shared_ptr<arrow::Field> schema)
      : chunked_array(std::move(chunked_array)), schema(std::move(schema)) {}

  std::unique_ptr<ArrowSchema> c_schema();
  CMetaflowArray chunk(int64_t c_idx);
  std::unique_ptr<ArrowChunkedArray> c_chunked_array();

  std::shared_ptr<arrow::ChunkedArray> chunked_array;
  std::shared_ptr<arrow::Field> schema;
};

struct CMetaflowDataFrame {
  CMetaflowDataFrame(std::shared_ptr<arrow::Table> table)
      : table(std::move(table)) {}

  std::unique_ptr<ArrowSchema> c_schema();
  CMetaflowChunkedArray column(int64_t c_idx);

  std::shared_ptr<arrow::Table> table;
};

#endif