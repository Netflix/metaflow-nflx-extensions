#include "select_rows.h"

#include "arrow/builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/table.h"
#include "arrow/util/thread_pool.h"

#include "util.h"
#include "indicator.h"

namespace {
// Scope guard to temporarily resize global arrow thread pool.
class CpuThreadPoolGuard {
public:
  CpuThreadPoolGuard(int threads)
      : original_threads_(arrow::GetCpuThreadPoolCapacity()),
        reset_(threads > 0) {
    if (threads > 0) {
      ops::check_status(arrow::SetCpuThreadPoolCapacity(threads));
    }
  }

  ~CpuThreadPoolGuard() {
    if (reset_) {
      auto status = arrow::SetCpuThreadPoolCapacity(original_threads_);
    }
  }

private:
  int original_threads_;
  bool reset_;
};

// Given a vector of monotonically increasing indices and a start point
// in the vector, find the next run of contiguous indices (i.e. where
// indices[j+1] = indices[j] + 1) of length at least min_run_threshold
// and return the start and end point.  If there are no sufficiently
// long runs return {-1, -1}.
std::pair<int64_t, int64_t>
find_next_slice_interval(const std::vector<int64_t> &indices, size_t start,
                         int64_t min_run_threshold) {
  if (start + min_run_threshold > indices.size()) {
    return std::make_pair(-1, -1);
  }

  // Checks if indices in the interval [start, end) form a
  // contiguous range.
  auto is_contiguous = [&](int64_t _start, int64_t _end) {
    return (_end - 1 < static_cast<int64_t>(indices.size())) &&
           (indices[_end - 1] == indices[_start] + _end - 1 - _start);
  };

  int64_t run_start = start;
  while (run_start + min_run_threshold <=
         static_cast<int64_t>(indices.size())) {
    int64_t run_end = run_start + min_run_threshold;
    if (is_contiguous(run_start, run_end)) {
      // Hit a run - extend as much as possible
      while (is_contiguous(run_start, run_end + min_run_threshold)) {
        run_end += min_run_threshold;
      }
      while (is_contiguous(run_start, run_end + 1)) {
        run_end += 1;
      }
      return std::make_pair(run_start, run_end);
    } else {
      // [run_start, run_end) is not a contiguous sequence but the tail
      // may be the start of a contiguous run.
      // Find smallest run_mid such that [run_mid, run_end) is contiguous
      int64_t run_mid = run_end - 1;
      while (is_contiguous(run_mid - 1, run_end)) {
        --run_mid;
      }
      if (is_contiguous(run_mid, run_mid + min_run_threshold)) {
        // Found a sufficiently large run starting at run_mid
        run_start = run_mid;
        continue;
      } else {
        // No contiguous run in tail of [run_start, run_end).
        // Jump ahead to run_end.
        run_start = run_end;
        continue;
      }
    }
  }
  // No runs found
  return std::make_pair(-1, -1);
}

// Create an arrow::Array with a view of a subrange of an int vector.
std::shared_ptr<arrow::Array>
array_from_view(const std::vector<int64_t> &values, size_t start, size_t end) {
  arrow::NumericBuilder<arrow::Int64Type> array_builder(
      arrow::default_memory_pool());
  ops::check_status(array_builder.AppendValues(values.data() + start, end - start));
  std::shared_ptr<arrow::Array> array;
  auto status = array_builder.Finish(&array);
  ops::check_status(status);
  return array;
}

std::shared_ptr<arrow::Table>
arrow_copy_table(std::shared_ptr<arrow::Table> table,
                 const std::vector<int64_t> &indices, int64_t start,
                 int64_t end) {
  // This should use arrow::dataset::Scanner::TakeRows() but TakeRows() does not
  // assume that indices are monotonically increasing so spends extra time
  // sorting indices, fetching data by ordered indices, then reshuffling back to
  // to the original index order.  The code below is closely modelled after
  // the implementation of TakeRows() without the sorting component.

  auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
  // Value copied from
  // https://github.com/apache/arrow/blob/9abd2b140813dfa941a592764ea07d38d2f0644e/cpp/src/arrow/dataset/scanner.h#L84
  // Empirically this keeps indices in CPU cache for multi-pass processing.
  const int64_t kDefaultBatchSize = 1 << 20;
  batch_reader->set_chunksize(kDefaultBatchSize);
  std::shared_ptr<arrow::RecordBatch> record_batch{nullptr};

  arrow::compute::ExecContext ctx(arrow::default_memory_pool());

  arrow::RecordBatchVector out_batches;
  int64_t indices_start = start;
  int64_t row_start = 0;

  while (true) {
    ops::check_status(batch_reader->ReadNext(&record_batch));
    if (!record_batch) {
      break;
    }

    const auto batch_rows = record_batch->num_rows();
    // TODO: avoid iterating over all batches for each copy
    if (row_start + batch_rows <= indices[indices_start]) {
      row_start += batch_rows;
      continue;
    } else if (static_cast<size_t>(end) < indices.size() &&
               row_start >= indices[end]) {
      break;
    }
    int64_t indices_end = indices_start + 1;
    while (indices_end < end && indices[indices_end] < row_start + batch_rows) {
      ++indices_end;
    }

    LOG << "Copying for run: [" << indices_start << ", " << indices_end << ")";

    arrow::Datum rel_indices =
        array_from_view(indices, indices_start, indices_end);
    rel_indices = ops::unwrap(
        arrow::compute::Subtract(rel_indices, arrow::Datum(row_start),
                                 arrow::compute::ArithmeticOptions(), &ctx));
    auto out_batch = ops::unwrap(
        arrow::compute::Take(record_batch, rel_indices,
                             arrow::compute::TakeOptions::Defaults(), &ctx));
    out_batches.push_back(out_batch.record_batch());

    indices_start = indices_end;
    if (indices_start >= end) {
      break;
    }
    row_start += batch_rows;
  }

  return ops::unwrap(
      arrow::Table::FromRecordBatches(table->schema(), std::move(out_batches)));
}

void copy_from_table(
    std::shared_ptr<arrow::Table> table,
    const std::vector<std::shared_ptr<arrow::Table>> &column_tables,
    const std::vector<int64_t> &indicator_ones, int64_t run_start,
    int64_t run_end, std::vector<arrow::ArrayVector> *output,
    bool in_parallel = false) {
  if (run_end == run_start) {
    return;
  }

  LOG << "Copying for run: [" << run_start << ", " << run_end << ")";

  int64_t n_columns = output->size();

  if (in_parallel) {
    auto *pool = arrow::internal::GetCpuThreadPool();
    std::vector<arrow::Future<std::shared_ptr<arrow::Table>>> result_futures;
    result_futures.reserve(output->size());

    for (int col = 0; col < n_columns; ++col) {
      auto &col_table = column_tables[col];
      result_futures.push_back(*pool->Submit([col_table, &indicator_ones,
                                              run_start, run_end]() {
        return arrow_copy_table(col_table, indicator_ones, run_start, run_end);
      }));
    }

    for (int col = 0; col < n_columns; ++col) {
      std::shared_ptr<arrow::Table> col_copy =
          ops::unwrap(result_futures[col].result());
      std::shared_ptr<arrow::ChunkedArray> chunks = col_copy->column(0);
      for (auto &chunk : chunks->chunks()) {
        (*output)[col].push_back(chunk);
      }
    }
  } else {
    std::shared_ptr<arrow::Table> table_copy =
        arrow_copy_table(std::move(table), indicator_ones, run_start, run_end);
    // append table copy to output
    for (int col = 0; col < n_columns; ++col) {
      std::shared_ptr<arrow::ChunkedArray> col_chunked_array =
          table_copy->column(col);
      for (auto &chunk : col_chunked_array->chunks()) {
        (*output)[col].push_back(chunk);
      }
    }
  }
}

/*! \brief Helper function to slice a column chunk to help with
   `slice_from_table`
*/
void slice_from_column_chunk(
    const std::shared_ptr<arrow::ChunkedArray> &column_chunk, int64_t offset,
    int64_t length, arrow::ArrayVector *col_output) {
  // Trivial way:
  // output_chunks = column_chunk->Slice(offset, length).
  const arrow::ArrayVector &all_chunks = column_chunk->chunks();
  std::size_t num_chunks = all_chunks.size();

  // Find start_chunk to start slicing from.
  // TODO: Think of smart ways to avoid this linear scan since we are
  // doing contiguous slicing.
  std::size_t curr_chunk = 0;
  while (curr_chunk < num_chunks &&
         offset >= all_chunks[curr_chunk]->length()) {
    offset -= all_chunks[curr_chunk]->length();
    ++curr_chunk;
  }

  while (curr_chunk < num_chunks && length > 0) {
    LOG << "Slicing from offset: " << offset << " length: " << length
        << " in chunk: " << curr_chunk;

    col_output->push_back(all_chunks[curr_chunk]->Slice(offset, length));
    length -= (all_chunks[curr_chunk]->length() - offset);
    offset = 0;
    ++curr_chunk;
  }
}

/*! \brief Slice array data from a table's columns

  Slice arrays from a table's columns and store array chunks in a vector for
  each column.

  @param table The table from which to slice
  @param offset The offset to start a slice
  @param length The length of the slice
  @param output A vector where array chunks for each column can be
  appended
  @param in_parallel Whether to multiple threads

  @return None
*/
void slice_from_table(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>> &column_chunks,
    const int64_t offset, const int64_t length,
    std::vector<arrow::ArrayVector> *output, bool in_parallel = 0) {

  if (length == 0) {
    return;
  }

  LOG << "Slicing for run: [" << offset << ", " << offset + length << ")";

  assert(output->size() == column_chunks.size());

  if (in_parallel) {
    auto *pool = arrow::internal::GetCpuThreadPool();
    std::vector<arrow::Future<int>> result_futures;
    result_futures.reserve(column_chunks.size());

    for (std::size_t i = 0; i < column_chunks.size(); ++i) {
      const auto &column_chunk = column_chunks[i];
      auto *col_output = &(*output)[i];

      result_futures.push_back(
          *pool->Submit([&column_chunk, offset, length, col_output]() {
            slice_from_column_chunk(column_chunk, offset, length, col_output);
            return 0;
          }));
    }

    for (std::size_t i = 0; i < result_futures.size(); ++i) {
      result_futures[i].Wait();
    }
  } else {
    for (std::size_t i = 0; i < column_chunks.size(); ++i) {
      slice_from_column_chunk(column_chunks[i], offset, length, &(*output)[i]);
    }
  }
}

} // namespace

namespace ops {
std::unique_ptr<CMetaflowDataFrame>
select_rows(struct CMetaflowDataFrame *df,
            struct CMetaflowChunkedArray *indicator, int null_conversion_policy,
            unsigned int threads, unsigned int min_run_threshold) {
  // get underlying arrow types
  std::shared_ptr<arrow::Table> df_t = df->table;
  std::shared_ptr<arrow::ChunkedArray> indicator_t = indicator->chunked_array;

  int num_columns = df_t->num_columns();

  // Prepare a vector that will hold all the output arrays for each column.
  std::vector<arrow::ArrayVector> out_arrays(num_columns);

  std::vector<std::shared_ptr<arrow::ChunkedArray>> column_chunks(num_columns);
  for (int i = 0; i < num_columns; ++i) {
    column_chunks[i] = df_t->column(i);
  }
  // Prepare a vector of single-column tables
  std::vector<std::shared_ptr<arrow::Table>> column_tables(num_columns);
  for (int col = 0; col < num_columns; ++col) {
    // Convert to single-column table to accomodate arrow api
    arrow::FieldVector fv{df_t->schema()->field(col)};
    auto col_schema = std::make_shared<arrow::Schema>(std::move(fv));
    auto col_table = arrow::Table::Make(col_schema, {df_t->column(col)});
    column_tables[col] = std::move(col_table);
  }

  bool in_parallel = threads > 1;
  CpuThreadPoolGuard thread_pool_guard(threads);

  // Prepare a vector that holds all the ones in the indicator to decide
  // between slice vs copy the underlying column values.
  std::vector<int64_t> indicator_ones;
  auto status = RowIndexBuilder::build(indicator_t, null_conversion_policy,
                                       &indicator_ones);
  ops::check_status(status);
  if (indicator_ones.empty()) {
    std::shared_ptr<arrow::Table> out_t = df_t->Slice(0, 0);

    return ops::make_unique<CMetaflowDataFrame>(out_t);
  }

  int64_t start = 0;
  while (start < static_cast<int64_t>(indicator_ones.size())) {
    const std::pair<int64_t, int64_t> slice_indices =
        find_next_slice_interval(indicator_ones, start, min_run_threshold);
    int64_t slice_start = slice_indices.first;
    int64_t slice_end = slice_indices.second;
    if (slice_start < 0) {
      // No remaining slices.  Copy to end of indicator array.
      copy_from_table(df_t, column_tables, indicator_ones, start,
                      indicator_ones.size(), &out_arrays, in_parallel);
      start = indicator_ones.size();
    } else {
      if (slice_start > start) {
        copy_from_table(df_t, column_tables, indicator_ones, start, slice_start,
                        &out_arrays, in_parallel);
      }
      slice_from_table(column_chunks, indicator_ones[slice_start],
                       slice_end - slice_start, &out_arrays, in_parallel);
      start = slice_end;
    }
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> out_chunked_arrays;
  for (int i = 0; i < num_columns; ++i) {
    out_chunked_arrays.push_back(
        std::make_shared<arrow::ChunkedArray>(std::move(out_arrays[i])));
  }
  std::shared_ptr<arrow::Table> out_t =
      arrow::Table::Make(df_t->schema(), std::move(out_chunked_arrays));

  return ops::make_unique<CMetaflowDataFrame>(out_t);
}
} // namespace ops
