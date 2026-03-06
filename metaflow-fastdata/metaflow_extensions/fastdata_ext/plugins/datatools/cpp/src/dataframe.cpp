#include "dataframe.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"

#include "util.h"

namespace {
std::unique_ptr<ArrowSchema> export_schema(const arrow::Field &schema) {
  auto c_schema = ops::make_unique<ArrowSchema>();
  ops::check_status(arrow::ExportField(schema, c_schema.get()));
  return c_schema;
}

std::unique_ptr<ArrowSchema> export_schema(const arrow::Schema &schema) {
  auto c_schema = ops::make_unique<ArrowSchema>();
  ops::check_status(arrow::ExportSchema(schema, c_schema.get()));
  return c_schema;
}

std::unique_ptr<ArrowArray> export_array(const arrow::Array &array) {
  auto c_array = ops::make_unique<ArrowArray>();
  ops::check_status(arrow::ExportArray(array, c_array.get()));
  return c_array;
}
} // namespace

std::unique_ptr<ArrowArray> CMetaflowArray::c_array() {
  return export_array(*array);
}

std::unique_ptr<ArrowSchema> CMetaflowArray::c_schema() {
  return export_schema(*schema);
}

std::unique_ptr<ArrowSchema> CMetaflowChunkedArray::c_schema() {
  return export_schema(*schema);
}

CMetaflowArray CMetaflowChunkedArray::chunk(int64_t c_idx) {
  return CMetaflowArray(chunked_array->chunk(c_idx), schema);
}

std::unique_ptr<ArrowChunkedArray> CMetaflowChunkedArray::c_chunked_array() {
  auto out = ops::make_unique<ArrowChunkedArray>();
  out->n_chunks = chunked_array->num_chunks();
  // ArrowChunkedArray uses C-style memory management
  out->chunks = (ArrowArray **)malloc(out->n_chunks * sizeof(ArrowArray *));
  for (int i = 0; i < out->n_chunks; ++i) {
    try {
      out->chunks[i] = chunk(i).c_array().release();
    } catch (...) {
      for (int j = 0; j < i; ++j) {
        auto *c_chunk = out->chunks[j];
        if (c_chunk->release) {
          c_chunk->release(c_chunk);
          delete (c_chunk);
        }
      }
      free(out->chunks);
      throw;
    }
  }
  return out;
}

std::unique_ptr<ArrowSchema> CMetaflowDataFrame::c_schema() {
  return export_schema(*table->schema());
}

CMetaflowChunkedArray CMetaflowDataFrame::column(int64_t c_idx) {
  return CMetaflowChunkedArray(table->column(c_idx), table->field(c_idx));
}