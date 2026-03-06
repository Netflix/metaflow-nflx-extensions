#include "c_api.h"

#include <memory>

// Arrow bridge headaers are missing but using a cassert include so we
// add it here.
#include <cassert>
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/util/logging.h"
#include "arrow/util/key_value_metadata.h"
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "clog.h"
#include "column.h"
#include "dataframe.h"
#include "list_to_tensor.h"
#include "parquet.h"
#include "select_rows.h"
#include "sparse_to_dense.h"
#include "struct_utils.h"
#include "util.h"

#define BEGIN_API()                                                            \
  char *exception = nullptr;                                                   \
  BEGIN_API_WITH_EXCEPTION(exception);

#define BEGIN_API_WITH_EXCEPTION(exception) try {

#define ON_ERROR(stmt)                                                         \
  }                                                                            \
  catch (const std::exception &ex) {                                           \
    if (exception) {                                                           \
      strcpy(exception, ex.what());                                            \
    }                                                                          \
    DEBUG("metaflow-data-api", "Exception: %s", ex.what());                    \
    stmt;                                                                      \
  }                                                                            \
  catch (...) {                                                                \
    if (exception) {                                                           \
      strcpy(exception, "Unknown error");                                      \
    }                                                                          \
    DEBUG("metaflow-data-api", "Unknown error");                               \
    stmt;                                                                      \
  }

namespace {

struct TmpArrayData {
  std::vector<void *> buffers;
  bool owns_buffers;
};

void release_tmp_array(ArrowArray *array) {
  if (array->release != nullptr) {
    TmpArrayData *tmp_data = (TmpArrayData *)array->private_data;
    for (void *buffer : tmp_data->buffers) {
      if (tmp_data->owns_buffers && buffer) {
        free(buffer);
      }
    }
    delete (tmp_data);
    array->release = nullptr;
  }
}
} // namespace

const char *md_version() { return "0.1.0"; }

void md_free_dataframe(CMetaflowDataFrame *df) { delete (df); }
void md_free_chunked_array(CMetaflowChunkedArray *array) { delete (array); }
void md_free_array(CMetaflowArray *array) { delete (array); }
void md_release_array(ArrowArray *array) {
  ArrowArrayRelease(array);
  delete (array);
}
void md_noop_release_schema(ArrowSchema * schema) {
  schema->release=nullptr;
}
void md_release_schema(ArrowSchema *schema) {
  ArrowSchemaRelease(schema);
  delete (schema);
}
void md_release_chunked_array(ArrowChunkedArray *chunked_array) {
  for (int i = 0; i < chunked_array->n_chunks; ++i) {
    auto *c_chunk = chunked_array->chunks[i];
    if (c_chunk && c_chunk->release) {
      c_chunk->release(c_chunk);
      delete (c_chunk);
    }
  }
  free(chunked_array->chunks);
  delete (chunked_array);
}

void md_free_memory(void *p) { free(p); }

CMetaflowChunkedArray *md_get_column(CMetaflowDataFrame *df, int64_t c_idx) {
  BEGIN_API();
  return new CMetaflowChunkedArray(df->column(c_idx));
  ON_ERROR(return nullptr);
}

CMetaflowChunkedArray *md_get_child(CMetaflowChunkedArray *array,
                                    int64_t c_idx) {
  BEGIN_API();
  auto &chunked_array = array->chunked_array;
  arrow::ArrayVector chunks;
  for (int i = 0; i < chunked_array->num_chunks(); ++i) {
    auto chunk = chunked_array->chunk(i);
    arrow::StructArray struct_chunk(chunk->data());
    auto child_chunk = struct_chunk.field(c_idx);
    chunks.push_back(std::move(child_chunk));
  }

  auto child_chunked_array =
      std::make_shared<arrow::ChunkedArray>(std::move(chunks));
  auto child_field = array->schema->type()->fields().at(c_idx);
  return new CMetaflowChunkedArray(std::move(child_chunked_array),
                                   std::move(child_field));

  ON_ERROR(return nullptr);
}

CMetaflowChunkedArray *md_get_chunk(CMetaflowChunkedArray *array,
                                    int64_t c_idx) {
  BEGIN_API();
  auto &chunked_array = array->chunked_array;
  auto single_chunk_array =
      std::make_shared<arrow::ChunkedArray>(chunked_array->chunk(c_idx));
  return new CMetaflowChunkedArray(std::move(single_chunk_array),
                                   array->schema);
  ON_ERROR(return nullptr);
}

ArrowChunkedArray *md_c_chunked_array(CMetaflowChunkedArray *chunked_array) {
  BEGIN_API();
  return chunked_array->c_chunked_array().release();
  ON_ERROR(return nullptr);
}

ArrowSchema *md_c_schema(CMetaflowDataFrame *df) {
  BEGIN_API();
  return df->c_schema().release();
  ON_ERROR(return nullptr);
}

ArrowSchema *md_c_schema_from_array(CMetaflowChunkedArray *array) {
  BEGIN_API();
  return array->c_schema().release();
  ON_ERROR(return nullptr);
}

ArrowChunkedArray *md_split_dataframe(CMetaflowDataFrame *df, int64_t max_chunksize) {
  BEGIN_API();
  arrow::TableBatchReader reader(*df->table);
  if (max_chunksize > 0) {
    reader.set_chunksize(max_chunksize);
  }
  std::vector<std::unique_ptr<ArrowArray>> batches;
  std::shared_ptr<arrow::RecordBatch> batch;

  while (true) {
    ops::check_status(reader.ReadNext(&batch));
    if (batch == nullptr) {
      break;
    }
    auto array = ops::make_unique<ArrowArray>();
    ops::check_status(arrow::ExportRecordBatch(*batch, array.get()));
    batches.push_back(std::move(array));
  }

  auto chunked_array = ops::make_unique<ArrowChunkedArray>();
  chunked_array->n_chunks = 0;
  struct ArrowArray **chunks =
      (struct ArrowArray **)malloc(batches.size() * sizeof(struct ArrowArray));
  if (chunks == nullptr) {
    return nullptr;
  }
  chunked_array->n_chunks = batches.size();
  for (int64_t i = 0; i < chunked_array->n_chunks; ++i) {
    chunks[i] = batches[i].release();
  }
  chunked_array->chunks = chunks;
  return chunked_array.release();
  ON_ERROR(return nullptr);
}

CMetaflowChunkedArray *md_array_from_data(const char *name, const char *format,
                                          void *data, void *mask,
                                          int64_t length, bool owns_buffers) {
  BEGIN_API();
  ArrowSchema schema{.format = format,
                     .name = name,
                     .metadata = nullptr,
                     .flags = 0,
                     .n_children = 0,
                     .children = nullptr,
                     .dictionary = nullptr,
                     .release = &md_noop_release_schema,
                     .private_data = nullptr};

  ArrowArray array{.length = length,
                   .null_count = 0,
                   .offset = 0,
                   .n_buffers = 2,
                   .n_children = 0,
                   .buffers = nullptr, // filled in below
                   .children = nullptr,
                   .dictionary = nullptr,
                   .release = &release_tmp_array,
                   .private_data = nullptr};

  std::unique_ptr<TmpArrayData> tmp_data = ops::make_unique<TmpArrayData>();
  tmp_data->buffers.resize(2);
  tmp_data->owns_buffers = owns_buffers;
  tmp_data->buffers[0] = mask;
  tmp_data->buffers[1] = data;

  array.buffers = (const void **)tmp_data->buffers.data();
  array.private_data = (void *)tmp_data.release();

  ArrowSchema importing_schema = schema;
  auto arrow_field = ops::unwrap(arrow::ImportField(&importing_schema));

  auto arrow_array = ops::unwrap(arrow::ImportArray(&array, &schema));

  auto arrow_chunked_array = std::make_shared<arrow::ChunkedArray>(arrow_array);

  return new CMetaflowChunkedArray(std::move(arrow_chunked_array),
                                   std::move(arrow_field));
  ON_ERROR(return nullptr);
}

void md_unpack_boolean(struct ArrowArray *array, int64_t buffer_idx,
                       bool is_null_buffer, uint8_t *out) {
  BEGIN_API();
  int64_t len = array->length;
  int64_t off = array->offset;
  uint8_t *buffer = (uint8_t *)array->buffers[buffer_idx];

  if (buffer == nullptr) {
    memset(out, 0, len);
  } else {
    for (int64_t i = 0; i < len; ++i) {
      int64_t j = i + off;
      if (is_null_buffer)
        out[i] = (buffer[j / 8] & (1 << (j % 8))) == 0;
      else
        out[i] = (buffer[j / 8] & (1 << (j % 8))) != 0;
    }
  }
  ON_ERROR(return );
}

CMetaflowDataFrame *md_df_from_struct_array(CMetaflowChunkedArray *array) {
  BEGIN_API();
  auto table =
      ops::unwrap(arrow::Table::FromChunkedStructArray(array->chunked_array));
  return new CMetaflowDataFrame(std::move(table));
  ON_ERROR(return nullptr);
}

CMetaflowChunkedArray *md_flatten_list_array(CMetaflowChunkedArray *array) {
  BEGIN_API();
  auto &chunked_array = array->chunked_array;
  arrow::ArrayVector chunks;
  for (auto i = 0; i < chunked_array->num_chunks(); ++i) {
    auto chunk = chunked_array->chunk(i);
    if (chunk->type()->name() == "large_list") {
      arrow::LargeListArray list_chunk(chunk->data());
      auto flattened_chunk = ops::unwrap(list_chunk.Flatten());
      chunks.push_back(std::move(flattened_chunk));
    } else {
      arrow::ListArray list_chunk(chunk->data());
      auto flattened_chunk = ops::unwrap(list_chunk.Flatten());
      chunks.push_back(std::move(flattened_chunk));
    }
  }

  auto child_chunked_array =
      std::make_shared<arrow::ChunkedArray>(std::move(chunks));
  auto child_field = array->schema->type()->fields().front();
  return new CMetaflowChunkedArray(std::move(child_chunked_array),
                                   std::move(child_field));

  ON_ERROR(return nullptr);
}

// Flatten a list column recursively into a view of its underlying type
struct CMetaflowChunkedArray *md_flatten_list_array_recursively(struct CMetaflowChunkedArray *array) {
  BEGIN_API();
  auto &chunked_array = array->chunked_array;
  arrow::ArrayVector chunks;
  for (int i = 0; i < chunked_array->num_chunks(); ++i) {
    auto curr_chunk = chunked_array->chunk(i);
    // TODO: see if we can improve this by grabbing child data directly
    while (curr_chunk->type_id() == arrow::Type::LARGE_LIST or curr_chunk->type_id() == arrow::Type::LIST) {
      if (curr_chunk->type_id() == arrow::Type::LARGE_LIST) {
        auto list_chunk = std::dynamic_pointer_cast<arrow::LargeListArray>(curr_chunk);
        curr_chunk = ops::unwrap(list_chunk->Flatten());
      }
      else {
        auto list_chunk = std::dynamic_pointer_cast<arrow::ListArray>(curr_chunk);
        curr_chunk = ops::unwrap(list_chunk->Flatten());
      }
    }
    chunks.push_back(std::move(curr_chunk));
  }

  auto data_chunked_array =
      std::make_shared<arrow::ChunkedArray>(std::move(chunks));
  std::shared_ptr<arrow::Field> data_field = array->schema->WithType(data_chunked_array->type());

  return new CMetaflowChunkedArray(std::move(data_chunked_array),
                                   std::move(data_field));
  ON_ERROR(return nullptr);
}

CMetaflowChunkedArray *
md_project_struct_list_array(CMetaflowChunkedArray *array,
                             const char **child_columns, int num_child_columns,
                             bool promote_single_column) {
  BEGIN_API();
  return ops::project_struct_list_array(array, child_columns, num_child_columns,
                                        promote_single_column)
      .release();
  ON_ERROR(return nullptr);
}

CMetaflowDataFrame *md_dataframe_from_chunked_arrays(
    CMetaflowChunkedArray **columns, int64_t n_arrays, char **names, char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);

  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
  arrow::FieldVector fields;

  for (int64_t i = 0; i < n_arrays; ++i) {
    chunked_arrays.push_back(columns[i]->chunked_array);
    fields.push_back(arrow::field(names[i], columns[i]->schema->type()));
  }

  std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, chunked_arrays);

  return new CMetaflowDataFrame(std::move(table));
  ON_ERROR(return nullptr);
}


CMetaflowDataFrame *md_stack_dataframes(CMetaflowDataFrame **dfs,
                                        unsigned int n_dfs,
                                        char* exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  std::vector<std::shared_ptr<arrow::Table>> tables;
  for (int64_t i = 0; i < n_dfs; ++i) {
    tables.push_back(dfs[i]->table);
  }

  if (tables.empty()) {
    std::shared_ptr<arrow::Schema> empty_schema =
        ops::unwrap(arrow::SchemaBuilder().Finish());
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(
        empty_schema, std::vector<std::shared_ptr<arrow::ChunkedArray>>());
    return new CMetaflowDataFrame(std::move(table));
  }

  return new CMetaflowDataFrame(ops::unwrap(arrow::ConcatenateTables(tables)));
  ON_ERROR(return nullptr);
}

CMetaflowDataFrame *md_add_null_column(CMetaflowDataFrame *df, const char *name,
                                       const char *arrow_type,
                                       CMetaflowChunkedArray *column,
                                       char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  return ops::add_null_column(*df, name, arrow_type, column);
  ON_ERROR(return nullptr);
}

int md_is_same_value(struct CMetaflowChunkedArray *array,
                     const int64_t * value_int,
                     const char * value_bytes,
                     char *exception)
{
  BEGIN_API_WITH_EXCEPTION(exception);
  return ops::is_same_value(array, value_int, value_bytes);
  ON_ERROR(return -1);
}

// Construct a new dataframe by dropping a top-level column by name.
// Name must be a column in input df.
struct CMetaflowDataFrame *md_drop_columns(struct CMetaflowDataFrame *df,
                                           const char **columns,
                                           int64_t num_columns,
                                           char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  std::shared_ptr<arrow::Table> table = df->table;
  for (auto idx = 0; idx < num_columns; ++idx) {
    const std::string name(columns[idx]);
    const auto column_idx = table->schema()->GetFieldIndex(name);
    if (column_idx == -1) {
      throw std::runtime_error("Column: " + name +
                               " does not exist in dataframe");
    }
    table = ops::unwrap(table->RemoveColumn(column_idx));
  }
  return new CMetaflowDataFrame(table);
  ON_ERROR(return nullptr);
}

int md_get_field_id(struct CMetaflowChunkedArray *array) {
  BEGIN_API();
  if (!array->schema->HasMetadata()) {
    return -1;
  }
  auto meta = array->schema->metadata();
  int pos = meta->FindKey("PARQUET:field_id");
  if (pos == -1) {
    return -1;
  }
  return std::stoi(meta->value(pos));
  ON_ERROR(return -1);
}

CMetaflowDataFrame *md_dataframe_from_record_batches(ArrowSchema *schema,
                                                     ArrowArray **batches,
                                                     int64_t n_batches) {
  BEGIN_API();
  std::shared_ptr<arrow::Schema> arrow_schema =
      ops::unwrap(arrow::ImportSchema(schema));
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_batches;
  for (int64_t i = 0; i < n_batches; ++i) {
    auto arrow_batch =
        ops::unwrap(arrow::ImportRecordBatch(batches[i], arrow_schema));
    arrow_batches.push_back(std::move(arrow_batch));
  }
  return new CMetaflowDataFrame(ops::unwrap(
      arrow::Table::FromRecordBatches(arrow_schema, arrow_batches)));

  ON_ERROR(return nullptr);
}

CMetaflowDataFrame *md_decode_parquet(const char *file_path,
                                      const char **columns,
                                      const int32_t *field_ids,
                                      int64_t num_columns_or_fields,
                                      bool allow_missing,
                                      char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  return ops::read_parquet(file_path, columns, field_ids, num_columns_or_fields, allow_missing).release();
  ON_ERROR(return nullptr);
}

struct CMetaflowDataFrame *md_decode_parquet_with_schema(const char *file_path,
                                                         struct ArrowSchema* c_schema,
                                                         char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  return ops::read_parquet_with_schema(file_path, c_schema).release();
  ON_ERROR(return nullptr);
}

void md_encode_parquet(const char *file_path,
                       struct CMetaflowDataFrame *df,
                       const char *compression_type,
                       int64_t offset,
                       int64_t length,
                       int64_t num_fields,
                       const int64_t* field_ids,
                       const char** field_paths,
                       bool use_int96_timestamp,
                       char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  ops::write_parquet(file_path,
                     df,
                     compression_type,
                     offset,
                     length,
                     num_fields,
                     field_ids,
                     field_paths,
                     use_int96_timestamp);
  ON_ERROR(return );
}

CMetaflowDataFrame *md_select_rows(struct CMetaflowDataFrame *df,
                                   struct CMetaflowChunkedArray *indicator,
                                   int null_conversion_policy,
                                   unsigned int threads,
                                   unsigned int min_run_threshold,
                                   char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  return ops::select_rows(df, indicator, null_conversion_policy, threads,
                          min_run_threshold)
      .release();
  ON_ERROR(return nullptr);
}

// Check if the array is a well-formed tensor, i.e. this
// is a list type (possibly nested) where the base type is a primitive
// and that the tensor is not ragged.
void md_check_tensor_format(struct ArrowArray *array,
                            struct ArrowSchema *schema,
                            bool *is_well_formed) {
  BEGIN_API();
  ops::check_tensor_format(array, schema, is_well_formed);
  ON_ERROR(return );
}

// Extract the shape of the tensor.
// Assumes that the array is a well-formed tensor according to md_check_tensor_format.
void md_get_tensor_shape(struct ArrowArray *array, int64_t num_dims, int64_t *shape) {
  BEGIN_API();
  return ops::get_tensor_shape(array, num_dims, shape);
  ON_ERROR(return );
}

// Check if the list is a well-formed tensor, i.e. this
// is a list type (possibly nested) where the base type is a primitive
// and that the tensor is not ragged.
void md_check_list_lengths(struct ArrowArray *array, struct ArrowSchema *schema,
                           int64_t len, bool *needs_padding,
                           bool *needs_trunctating) {
  BEGIN_API();
  ops::check_list_lengths(array, schema, len, needs_padding, needs_trunctating);
  ON_ERROR(return );
}

// Extract the shape of the tensor.
// Assumes that the list is a well-formed tensor.
void md_list_to_tensor(struct ArrowArray *array, struct ArrowSchema *schema,
                       int64_t itemsize, const char* target_type, double null_val, double pad_val,
                       void *out) {
  BEGIN_API();
  ops::list_to_tensor(array, schema, itemsize, target_type, null_val, pad_val, out);
  ON_ERROR(return );
}

void md_sparse_to_dense(struct ArrowArray *indices,
                        struct ArrowSchema *indices_schema,
                        struct ArrowArray *values,
                        struct ArrowSchema *values_schema, int *shape,
                        int shape_len, double null_val, void *out) {
  BEGIN_API();
  ops::sparse_to_dense(*indices, *indices_schema, *values, *values_schema,
                       shape, shape_len, null_val, out);
  ON_ERROR(return );
}

CMetaflowDataFrame *md_rename_dataframe(CMetaflowDataFrame *df,
                                        const char **names,
                                        char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  std::shared_ptr<arrow::Table> table = df->table;
  int num_cols = table->num_columns();
  std::vector<std::string> new_names;
  for (int i = 0; i < num_cols; ++i) {
    new_names.push_back(std::string(names[i]));
  }
  std::shared_ptr<arrow::Table> renamed_table = ops::unwrap(table->RenameColumns(new_names));
  return ops::make_unique<CMetaflowDataFrame>(std::move(renamed_table)).release();
  ON_ERROR(return nullptr);
}

// Write the dataframe into a buffer
void md_write_dataframe_to_buffer(struct CMetaflowDataFrame *df, uint8_t *buffer, uint64_t buffer_size, uint64_t *data_len, char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  // Initialize the data_len
  *data_len = 0;

  // Convert the buffer to an Arrow OutputStream
  auto arrow_buffer = std::make_shared<arrow::MutableBuffer>(buffer, buffer_size);
  auto output_stream = std::make_shared<arrow::io::FixedSizeBufferWriter>(arrow_buffer);
  std::shared_ptr<arrow::Table> table = df->table;

  // Create recordBatchStreamWriter
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> result = arrow::ipc::MakeStreamWriter(output_stream.get(), table->schema());
  writer = result.ValueOrDie();

  ops::check_status(writer->WriteTable(*table));
  ops::check_status(writer->Close());

  // Update the number of bytes written
  *data_len = output_stream->Tell().ValueOrDie();
  ON_ERROR(return );
}

struct CMetaflowDataFrame *md_read_dataframe_from_buffer(uint8_t *buffer, uint64_t buffer_size, char *exception) {
  BEGIN_API_WITH_EXCEPTION(exception);
  // Convert the buffer to an Arrow InputStream
  auto arrow_buffer = std::make_shared<arrow::Buffer>(buffer, buffer_size);
  auto input_stream = std::make_shared<arrow::io::BufferReader>(arrow_buffer);

  // initialize variables
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> reader_result;

  // Create recordBatchStreamReader
  reader_result = arrow::ipc::RecordBatchStreamReader::Open(input_stream.get());
  if (!reader_result.ok()) {
    // Handle error: buffer is not a valid IPC buffer
    throw std::runtime_error("Error opening RecordBatchStreamReader: " +  reader_result.status().ToString());
  }
  reader = reader_result.ValueOrDie();

  std::shared_ptr<arrow::Table> table;
  arrow::Result<std::shared_ptr<arrow::Table>> table_result;

  // Read the table from the reader
  table_result = reader->ToTable();
  if (!table_result.ok()) {
    // Handle error: failed to read table from IPC buffer
    throw std::runtime_error("Error reading table from IPC buffer: " + table_result.status().ToString());
  }
  table = table_result.ValueOrDie();

  return new CMetaflowDataFrame(std::move(table));
  ON_ERROR(return nullptr);
}
