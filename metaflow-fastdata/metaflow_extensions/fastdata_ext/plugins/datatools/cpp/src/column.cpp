#include "column.h"

#include "arrow/array.h"
#include "arrow/visit_array_inline.h"
#include "arrow/builder.h"
#include "arrow/type.h"

#include "util.h"

#include <string_view>

namespace {
// Convert arrow C data format string to arrow::Field
std::shared_ptr<arrow::Field> make_field(const std::string &name,
                                         const char *arrow_type) {
  const std::string arrow_str(arrow_type);
#define FIELD(type_str, type_name)                                             \
  do {                                                                         \
    if (arrow_str == #type_str) {                                              \
      return std::make_shared<arrow::Field>(                                   \
          name, std::make_shared<arrow::type_name>());                         \
    }                                                                          \
  } while (0)

  FIELD(b, BooleanType);
  FIELD(c, Int8Type);
  FIELD(C, UInt8Type);
  FIELD(s, Int16Type);
  FIELD(S, UInt16Type);
  FIELD(i, Int32Type);
  FIELD(I, UInt32Type);
  FIELD(l, Int64Type);
  FIELD(L, UInt64Type);
  FIELD(f, FloatType);
  FIELD(g, DoubleType);
  FIELD(c, Int8Type);
  FIELD(z, BinaryType);
  FIELD(Z, LargeBinaryType);
  FIELD(u, StringType);
  FIELD(U, LargeStringType);
  FIELD(tdD, Date32Type);
  FIELD(tdm, Date64Type);
#undef FIELD

#define TS_FIELD(type_str, time_unit)                                          \
  do {                                                                         \
    if (arrow_str == #type_str) {                                              \
      return std::make_shared<arrow::Field>(                                   \
          name,                                                                \
          std::make_shared<arrow::TimestampType>(arrow::TimeUnit::time_unit)); \
    } else if (arrow_str.rfind(#type_str) == 0) {                              \
      const std::string timezone(arrow_str.begin() + strlen(#type_str),        \
                                 arrow_str.end());                             \
      return std::make_shared<arrow::Field>(                                   \
          name, std::make_shared<arrow::TimestampType>(                        \
                    arrow::TimeUnit::time_unit, timezone));                    \
    }                                                                          \
  } while (0)

  TS_FIELD(tss:, SECOND);
  TS_FIELD(tsm:, MILLI);
  TS_FIELD(tsu:, MICRO);
  TS_FIELD(tsn:, NANO);

#undef TS_FIELD

  throw std::runtime_error("Unsupported arrow type: " + arrow_str);
}


class IsSameValue
{
  private:
    union value_union {
        int64_t i;
        const char * c;
    };

    value_union _value;

  public:
    bool is_same;

    IsSameValue() : _value{0}, is_same(false) {}
    void set(int64_t value) { _value.i = value; }
    void set(const char * value) { _value.c = value; }

    arrow::Status
    operator()(std::shared_ptr<arrow::ChunkedArray> chunked_array){
      is_same = true;
      for (auto array : chunked_array->chunks()){
        ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array, this));
        if (is_same == false)
          return arrow::Status::OK();
      }
      return arrow::Status::OK();
    }

    // Default implementation
    arrow::Status Visit(const arrow::Array& array) {
      return arrow::Status::NotImplemented("Can not check values for array of type ",
                                           array.type()->ToString());
    }

    // signed integer type
    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_signed_integer<T, arrow::Status> Visit(const ArrayType& array) {
      for (int64_t i = 0; i < array.length(); ++i){
        if ((array.IsNull(i)) || (static_cast<int64_t>(array.Value(i)) != _value.i)) {
          is_same = false;
          return arrow::Status::OK();
        }
      }
      return arrow::Status::OK();
    }

    // string/byte type
    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_base_binary<T, arrow::Status> Visit(const ArrayType& array) {
      for (int64_t i = 0; i < array.length(); ++i){
        if (array.IsNull(i)) {
          is_same = false;
          return arrow::Status::OK();
        }

        std::string_view data = array.GetView(i);
        if (std::string_view(_value.c) != data) {
          is_same = false;
          return arrow::Status::OK();
        }
      }
      return arrow::Status::OK();
    }
};  // IsSameValue
} // namespace

namespace ops {
CMetaflowDataFrame *add_null_column(CMetaflowDataFrame &df, const char *c_name,
                                    const char *arrow_type,
                                    const CMetaflowChunkedArray *column) {
  if (arrow_type == nullptr && column == nullptr) {
    throw std::runtime_error(
        "Must supply either arrow_type or template column");
  }
  std::string name(c_name);

  std::shared_ptr<arrow::Field> field;
  if (arrow_type != nullptr) {
    field = make_field(name, arrow_type);
  } else {
    field = column->schema->WithName(name);
  }

  const auto table = df.table;

  arrow::TableBatchReader reader(*table);
  std::vector<std::unique_ptr<ArrowArray>> batches;
  std::shared_ptr<arrow::RecordBatch> batch;

  arrow::ArrayVector null_chunks;

  while (true) {
    ops::check_status(reader.ReadNext(&batch));
    if (batch == nullptr) {
      break;
    }
    std::unique_ptr<arrow::ArrayBuilder> builder;
    ops::check_status(arrow::MakeBuilder(arrow::default_memory_pool(),
                                         field->type(), &builder));

    ops::check_status(builder->AppendNulls(batch->num_rows()));
    std::shared_ptr<arrow::Array> array = ops::unwrap(builder->Finish());
    null_chunks.push_back(array);
  }

  auto chunked_array =
      ops::unwrap(arrow::ChunkedArray::Make(null_chunks, field->type()));
  return new CMetaflowDataFrame(ops::unwrap(
      table->AddColumn(table->num_columns(), field, chunked_array)));
}

bool is_same_value(struct CMetaflowChunkedArray *array,
                   const int64_t * value_int,
                   const char * value_bytes)
{
  if (array == nullptr)
    throw std::runtime_error("The array must be non-null");

  if ((value_int == nullptr) && (value_bytes == nullptr))
    throw std::runtime_error("the int value or the bytes value must be non-null");

  IsSameValue isv;
  if (value_int != nullptr)
    isv.set(*value_int);
  else
    isv.set(value_bytes);

  auto status = isv(array->chunked_array);
  if (status.ok())
    return isv.is_same;

  throw std::runtime_error("Error checking array values : " + status.ToString());
}
} // namespace ops
