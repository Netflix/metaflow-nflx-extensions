#include "arrow/c/bridge.h"
#include "arrow/util/key_value_metadata.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "arrow/dataset/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"

#include "parquet.h"
#include "parquet_patches.h"
#include "util.h"

namespace {
std::map<std::vector<std::string>, int64_t>
unpack_field_ids(int64_t num_fields, const int64_t *field_ids,
                 const char **field_paths) {
  std::map<std::vector<std::string>, int64_t> field_id_map;
  int32_t field_paths_idx = 0;
  for (int32_t i = 0; i < num_fields; ++i) {
    std::vector<std::string> path;
    while (field_paths[field_paths_idx] != nullptr) {
      path.push_back(std::string(field_paths[field_paths_idx]));
      field_paths_idx += 1;
    }
    field_paths_idx += 1;
    field_id_map[path] = field_ids[i];
  }
  return field_id_map;
}
} // namespace

namespace ops {
std::unique_ptr<CMetaflowDataFrame>
read_parquet(const char *file_path, const char **columns,
             const int32_t *field_ids, int64_t num_columns_or_fields , bool allow_missing) {

  if ((columns != nullptr) && (field_ids != nullptr))
    throw std::runtime_error("Cannot set both arguments 'columns' and "
                             "'field_ids' to non-null pointers");

  std::shared_ptr<arrow::Table> table;

  // Open file
  auto pool = arrow::default_memory_pool();
  std::string file = std::string(file_path);
  std::shared_ptr<arrow::io::ReadableFile> infile =
    ops::unwrap(arrow::io::ReadableFile::Open(file, pool));
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ops::check_status(parquet::arrow::OpenFile(infile, pool, &arrow_reader));

  // Process columns or field_ids
  std::shared_ptr<arrow::Schema> schema;
  ops::check_status(arrow_reader->GetSchema(&schema));
  std::vector<int> idx;

  // Columns
  if ((num_columns_or_fields > 0) && (columns != nullptr)) {
    for (int64_t i = 0; i < num_columns_or_fields; ++i)
      idx.push_back(schema->GetFieldIndex(std::string(columns[i])));
  }
  // Field ids
  else if ((num_columns_or_fields > 0) && (field_ids != nullptr)) {
    // gather field ids or -1 if missing
    std::vector<int> table_field_ids;
    for (int i = 0; i < schema->num_fields(); ++i) {
      if (schema->field(i)->HasMetadata()) {
        int const key = schema->field(i)->metadata()->FindKey("PARQUET:field_id");
        if (key > -1) {
          std::string value = schema->field(i)->metadata()->value(key);
          table_field_ids.push_back(std::stoi(value));
        }
        else {
          table_field_ids.push_back(-1);
        }
      }
      else {
        table_field_ids.push_back(-1);
      }
    }

    // find the position of each field id
    for (int i = 0; i < num_columns_or_fields; ++i) {
      auto pos = std::find(table_field_ids.begin(), table_field_ids.end(), field_ids[i]);
      if (pos != table_field_ids.end())
        idx.push_back(pos - table_field_ids.begin());
      else
        idx.push_back(-1);
    }
  }

  // Throw if missing
  if (!allow_missing) {
    if (std::any_of(idx.begin(), idx.end(), [](int i) { return i == -1; })) {
      std::vector<std::string> missing;
      std::string type;
      for (int64_t i = 0; i < num_columns_or_fields; ++i) {
        if (idx[i] == -1) {
          if (columns != nullptr) {
            type = "column";
            missing.push_back(std::string(columns[i]));
          }
          else if (field_ids != nullptr) {
            type = "field id";
            missing.push_back(std::to_string(field_ids[i]));
          }
        }
      }
      std::string error_msg = "Requested " + type + "(s) not found: " +
        ops::join_str(missing, ", ") + ".";
      throw std::runtime_error(error_msg);
    }
  }

  if (idx.size() > 0) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto col_idx : idx) {
      if (col_idx > -1) {
        auto field = schema->field(col_idx);
        auto chunked_array =
          ops::unwrap(arrow::ChunkedArray::Make({}, field->type()));
        ops::check_status(arrow_reader->ReadColumn(col_idx, &chunked_array));
        chunked_arrays.push_back(chunked_array);
        fields.push_back(field);
      }
    }

    auto filtered_schema = std::make_shared<arrow::Schema>(std::move(fields));
    table = arrow::Table::Make(filtered_schema, std::move(chunked_arrays));
  }
  else {
    ops::check_status(arrow_reader->ReadTable(&table));
  }

  return ops::make_unique<CMetaflowDataFrame>(std::move(table));
}

std::unique_ptr<CMetaflowDataFrame>
read_parquet_with_schema(const char *file_path, ArrowSchema* c_schema) {

  // If we have no schema, fail, otherwise import the c-schema to a c++ schema
  if (c_schema == nullptr) {
    throw std::runtime_error("Must have a valid c-style schema");
  }
  std::shared_ptr<arrow::Schema> schema = ops::unwrap(arrow::ImportSchema(c_schema));

  // Table for the result
  std::shared_ptr<arrow::Table> table;

  // Get path and file name
  std::vector<std::string> parts = split_str(std::string(file_path), std::string("/"));
  std::vector<std::string> base_path_parts = std::vector<std::string>(parts.begin(), parts.end()-1);
  std::string file = std::string(file_path);
  std::vector<std::string> file_paths = {file};
  std::string uri = "file://" + join_str(base_path_parts, "/");

  // Instantiate a file system data set
  std::shared_ptr<arrow::fs::FileSystem> fs = ops::unwrap(arrow::fs::FileSystemFromUri(uri));
  std::shared_ptr<arrow::dataset::FileFormat> format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  arrow::dataset::FileSystemFactoryOptions options;
  auto factory = ops::unwrap(arrow::dataset::FileSystemDatasetFactory::Make(fs, file_paths, format, options));

  // Get the data set with the specified schema
  std::shared_ptr<arrow::dataset::Dataset> dataset;
  if (schema != nullptr) {
    dataset = ops::unwrap(factory->Finish(schema));
  }
  else {
    dataset = ops::unwrap(factory->Finish());
  }

  // Build a scanner
  std::shared_ptr<arrow::dataset::ScannerBuilder> builder = ops::unwrap(dataset->NewScan());
  std::shared_ptr<arrow::dataset::Scanner> scanner = ops::unwrap(builder->Finish());

  // Get and return the table
  table = ops::unwrap(scanner->ToTable());
  return ops::make_unique<CMetaflowDataFrame>(std::move(table));
}

void write_parquet(const char *file_path, struct CMetaflowDataFrame *df,
                   const char *compression_type, int64_t offset, int64_t length,
                   int64_t num_fields, const int64_t *field_ids,
                   const char **field_paths, bool use_int96_timestamp) {

  auto pool = arrow::default_memory_pool();

  parquet::ArrowWriterProperties::Builder arrowWriterPropertiesBuilder;
  if (use_int96_timestamp) {
    arrowWriterPropertiesBuilder.enable_deprecated_int96_timestamps();
  }

  parquet::WriterProperties::Builder pqPropertiesBuilder;

  // The following compression types are not supported in our version of arrow:
  // BZ2, LZ4, LZO, LZ4_FRAME, LZ4_HADOOP
  std::string compression(compression_type);
  if (compression == "UNCOMPRESSED")
    pqPropertiesBuilder.compression(arrow::Compression::UNCOMPRESSED);
  else if (compression == "SNAPPY")
    pqPropertiesBuilder.compression(arrow::Compression::SNAPPY);
  else if (compression == "GZIP")
    pqPropertiesBuilder.compression(arrow::Compression::GZIP);
  else if (compression == "BROTLI")
    pqPropertiesBuilder.compression(arrow::Compression::BROTLI);
  else if (compression == "ZSTD")
    pqPropertiesBuilder.compression(arrow::Compression::ZSTD);
  else
    throw std::runtime_error("Unknown parquet compression: " + compression);

  std::shared_ptr<arrow::Table> table = df->table;
  // Extract field_id map
  std::map<std::vector<std::string>, int64_t> field_ids_map;
  const std::map<std::vector<std::string>, int64_t> *field_ids_ptr = nullptr;
  if (field_ids != NULL) {
    field_ids_map = unpack_field_ids(num_fields, field_ids, field_paths);
    field_ids_ptr = &field_ids_map;
  }

  // slice our table if needed
  if ((offset > -1) && (length > -1)) {
    table = table->Slice(offset, length);
  }

  // Similar to pyarrow, we set the row group size to the size of the table. It
  // will then choose between this and the max row group length property
  // (defaults to 64Mb)
  // https://arrow.apache.org/docs/cpp/api/formats.html#_CPPv4N7parquet16WriterProperties7Builder20max_row_group_lengthE7int64_t
  int64_t chunk_size(df->table->num_rows());

  std::string file(file_path);
  std::shared_ptr<arrow::io::FileOutputStream> outfile =
      ops::unwrap(arrow::io::FileOutputStream::Open(file));
  ops::check_status(ops::patches::WriteTable(
      *table, pool, outfile, chunk_size, pqPropertiesBuilder.build(),
      arrowWriterPropertiesBuilder.build(), field_ids_ptr));
}
} // namespace ops
