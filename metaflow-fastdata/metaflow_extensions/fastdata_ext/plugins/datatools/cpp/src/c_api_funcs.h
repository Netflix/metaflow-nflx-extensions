// DO NOT DIRECTLY INCLUDE THIS FILE
// Use c_api.h instead.  This file is shared between c_api.h and the python
// CFFI loader for consistency and should not be used elsewhere.

const char *md_version();

// Memory management: All structs must be freed with the following methods
// when done.
void md_free_dataframe(struct CMetaflowDataFrame *);
void md_free_chunked_array(struct CMetaflowChunkedArray *);
void md_free_array(struct CMetaflowArray *);
void md_release_array(struct ArrowArray *);
void md_release_schema(struct ArrowSchema *);
void md_noop_release_schema(struct ArrowSchema *);
void md_release_chunked_array(struct ArrowChunkedArray *);

// Free arbitrary memory (TODO: remove when not needed)
void md_free_memory(void *);

// Accessors and convertors.  Returned objects have shared ownership of
// underlying data and must be freed with the corresponding methods above.
struct CMetaflowChunkedArray *md_get_column(struct CMetaflowDataFrame *df,
                                            int64_t c_idx);

struct CMetaflowChunkedArray *md_get_child(struct CMetaflowChunkedArray *array,
                                           int64_t c_idx);

struct CMetaflowChunkedArray *md_get_chunk(struct CMetaflowChunkedArray *array,
                                           int64_t c_idx);

struct ArrowChunkedArray *
md_c_chunked_array(struct CMetaflowChunkedArray *array);
struct ArrowSchema *md_c_schema(struct CMetaflowDataFrame *);
struct ArrowSchema *md_c_schema_from_array(struct CMetaflowChunkedArray *array);

// Split a dataframe into chunks, with each chunk exposed as a multi-column
// ArrowArray.
struct ArrowChunkedArray *md_split_dataframe(struct CMetaflowDataFrame *, int64_t max_chunksize);

// Construct a single-chunk arrow array from contiguous data.
// Returned object must be freed with md_free_chunked_array regardless of
// whether it owns its buffers.
struct CMetaflowChunkedArray *md_array_from_data(const char *name,
                                                 const char *format, void *data,
                                                 void *mask, int64_t length,
                                                 bool owns_buffers);

// Convert bit-packed boolean buffer to uint8_t array.  Out array must have
// sufficient memory reserved.
void md_unpack_boolean(struct ArrowArray *array, int64_t buffer_idx,
                       bool is_null_buffer, uint8_t *out);

// Construct a dataframe from a struct column promoting struct fields to
// columns in the new dataframe.
struct CMetaflowDataFrame *
md_df_from_struct_array(struct CMetaflowChunkedArray *);

// Flatten a list column into a view of its underlying type
struct CMetaflowChunkedArray *
md_flatten_list_array(struct CMetaflowChunkedArray *);

// Given a chunked array of type List<Struct<...>> and a list of column
// names of the inner struct, return a new chunked array of type
// List<Struct<{selected columns}>>.
// If promote_single_column is true and exactly 1 column is requested
// the returned array has type List<selected_column>.
struct CMetaflowChunkedArray *
md_project_struct_list_array(struct CMetaflowChunkedArray *array,
                             const char **child_columns, int num_child_columns,
                             bool promote_single_column);

// Construct a dataframe from RecordBatches, with each RecordBatch provided
// as a multi-column ArrowArray.
struct CMetaflowDataFrame *md_dataframe_from_record_batches(
    struct ArrowSchema *schema, struct ArrowArray **batches, int64_t n_batches);

// Construct a dataframe from a list of chunked arrays.
struct CMetaflowDataFrame *md_dataframe_from_chunked_arrays(
    struct CMetaflowChunkedArray **columns, int64_t n_arrays, char **names, char *exception);

// Concatenate dataframes.  Dataframes should all have the same schema.
struct CMetaflowDataFrame *md_stack_dataframes(struct CMetaflowDataFrame **dfs,
                                               unsigned int n_dfs,
                                               char* exception);

// Construct a new dataframe by adding an all-null column with specified type.
// Column type can be specified either with an arrow format string or
// with a chunked array.
struct CMetaflowDataFrame *
md_add_null_column(struct CMetaflowDataFrame *df, const char *name,
                   const char *arrow_type, struct CMetaflowChunkedArray *column,
                   char *exception);

// Check if a columns rows are equal to a constant value
int md_is_same_value(struct CMetaflowChunkedArray *array,
                     const int64_t * value_int,
                     const char * value_bytes,
                     char *exception);

// Construct a new dataframe by dropping top-level columns by name.
// Column names must exist in the input dataframe.
struct CMetaflowDataFrame *md_drop_columns(struct CMetaflowDataFrame *df,
                                           const char **columns,
                                           int64_t num_columns,
                                           char *exception);

// Get the field id from a chunked array
int md_get_field_id(struct CMetaflowChunkedArray *array);

// Read parquet file into a dataframe, optionally projecting to specified
// columns.
struct CMetaflowDataFrame *md_decode_parquet(const char *file_path,
                                             const char **columns,
                                             const int32_t *field_ids,
                                             int64_t num_columns_or_fields,
                                             bool allow_missing,
                                             char *exception);

// Read parquet file into a dataframe, optionally projecting to
// specified schemas.
struct CMetaflowDataFrame *md_decode_parquet_with_schema(const char *file_path,
                                                         struct ArrowSchema* c_schema,
                                                         char *exception);

// Write dataframe into parquet file into a dataframe.
void md_encode_parquet(const char *file_path,
                       struct CMetaflowDataFrame *df,
                       const char *compression_type,
                       int64_t offset,
                       int64_t length,
                       int64_t num_fields,
                       const int64_t* field_ids,
                       const char** field_paths,
                       bool use_int96_timestamp,
                       char *exception);

// Select rows from a dataframe using an indicator array.
struct CMetaflowDataFrame *
md_select_rows(struct CMetaflowDataFrame *df,
               struct CMetaflowChunkedArray *indicator,
               int null_conversion_policy, unsigned int threads,
               unsigned int min_run_threshold, char *exception);

// Checks lengths of entries in a list array against expected length.
// Sets needs_padding to true if any list entries are null or are shorter than
// expected length.  Sets needs_truncating to true if any list entries are
// longer than expected lengths.  Does not check for null values within lists.
void md_check_list_lengths(struct ArrowArray *array, struct ArrowSchema *schema,
                           int64_t len, bool *needs_padding,
                           bool *needs_trunctating);

// Convert a list of primitive values to a tensor with target arrow type.
// Tensor is assumed to be 2-D with dimensions (nrows) x (itemsize).
// For multidimensional tensors users must rehsape output.
// Output must have sufficient space allocated by user.
void md_list_to_tensor(struct ArrowArray *array, struct ArrowSchema *schema,
                       int64_t itemsize, const char *target_type, double null_val,
                       double pad_val, void *out);

// Convert a sparse tensor representation to a dense tensor array.
// Sparse tensors are given by an integral indices array and a primitive
// values array.  Indices can be either 1-D indices into a flattened
// view of the dense tensor or multi-dimensional indices, in which
// case the index array must consist of concatenated tuples of indices
// (so len(indices) == len(values) * (shape_len)).
// User must allocate sufficient memory in output and fill in default value.
void md_sparse_to_dense(struct ArrowArray *indices,
                        struct ArrowSchema *indices_schema,
                        struct ArrowArray *values,
                        struct ArrowSchema *values_schema, int *shape,
                        int shape_len, double null_val, void *out);

// rename the column names in a dataframe.
struct CMetaflowDataFrame *md_rename_dataframe(struct CMetaflowDataFrame *df,
                                               const char **names,
                                               char *exception);

// Convert an iceberg manifest list or manifest file to json.
void md_iceberg_to_json(const char* filename,
                        const char* type,
                        char** metadata,
                        char*** json_str,
                        int64_t* size,
                        char* exception);

// Check if the array is a well-formed tensor, i.e. this
// is a list type (possibly nested) where the base type is a primitive
// and that the tensor is not ragged.
void md_check_tensor_format(struct ArrowArray *array,
                            struct ArrowSchema *schema,
                            bool * is_well_formed);


// Extract the shape of the tensor.
// Assumes that the array is a well-formed tensor according to md_check_tensor_format.
void md_get_tensor_shape(struct ArrowArray *array, int64_t num_dims, int64_t *shape);

// Flatten a list column recursively into a view of its underlying type
struct CMetaflowChunkedArray *md_flatten_list_array_recursively(struct CMetaflowChunkedArray *array);

// Write the dataframe into a memoryview buffer
void md_write_dataframe_to_buffer(struct CMetaflowDataFrame *df, uint8_t *buffer, uint64_t buffer_size, uint64_t* data_len, char *exception);

// Read the dataframe from a memoryview buffer
struct CMetaflowDataFrame *md_read_dataframe_from_buffer(uint8_t *buffer, uint64_t buffer_size, char *exception);
