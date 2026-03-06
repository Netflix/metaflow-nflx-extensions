#ifndef METAFLOW_DATA_PARQUET_PATCHES
#define METAFLOW_DATA_PARQUET_PATCHES

#include "arrow/status.h"
#include "parquet/arrow/writer.h"

// This file contains patches to arrow's C++ Parquet reader which optionally
// adds field ids to the parquet metadata, overriding the current default in
// arrow which is to set field_id=-1, i.e. field ids can be assigned
// arbitrarily. Arrow's parquet writer currently does not expose a way to
// provide field ids, so we need to patch its code instead.

namespace ops {
namespace patches {
arrow::Status
WriteTable(const ::arrow::Table &table, ::arrow::MemoryPool *pool,
           std::shared_ptr<::arrow::io::OutputStream> sink, int64_t chunk_size,
           std::shared_ptr<parquet::WriterProperties> properties,
           std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties,
           const std::map<std::vector<std::string>, int64_t> *field_ids);
} // namespace patches
} // namespace ops

#endif