#ifndef ARROWOPS_INDICATOR_H
#define ARROWOPS_INDICATOR_H

#include "arrow/array.h"
#include "log.h"

namespace ops {
/* Helper macro to add a virtual Visit method to IndicatorBuilderIndices that
 * specializes the process method for the specific TYPE_CLASS */
#define ROW_INDEX_BUILDER_VISITOR(TYPE_CLASS)                                  \
  virtual arrow::Status Visit(const TYPE_CLASS &type) {                        \
    typedef typename arrow::TypeTraits<TYPE_CLASS>::ArrayType ArrayType;       \
    return process<ArrayType>();                                               \
  }

/* Macro to define Visitor for non-implemented types that gives a more clear
 * error message than the arrow default that just prints the type without any
 * other human readable component.*/
#define ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(TYPE_CLASS)                         \
  virtual arrow::Status Visit(const TYPE_CLASS &type) {                        \
    return arrow::Status::NotImplemented(                                      \
        "Indicator arrays of type '", type.ToString(),                         \
        "' are not supported. Indicator arrays must be boolean or integer "    \
        "arrays.");                                                            \
  }

/* \brief This class facilitiates building a vector of indices from a
 * boolean-like indicator array. This class is used internally to the
 * ops::select_rows(...) function.

 An indicator array is an array that determines which rows are selected from an
 arrow table. Indictor arrays are used as arguments to the
 ops::select_rows(...) function.  This class is used to help transform an
 indicator array into a std::vector of row indices. The resultant vector will
 contain a monotonically non-repeating sequence of row indices.
*/
class RowIndexBuilder : public arrow::TypeVisitor {
public:
  /* \brief Build a row index vector from an indicator array

     @param in_array A chunked array representing the indicator array

     @param null_conversion_policy The null conversion policy. '-1' indicates to
     throw an exception if a null value is encountered. '1' indicates to select
     rows where null-values are present in the indicator. '0' indicates to not
     select rows where the null-values are present in the indicator.

     @param indices A vector to be populated with row indices

   */
  static arrow::Status
  build(const std::shared_ptr<arrow::ChunkedArray> &in_array,
        int null_conversion_policy, std::vector<int64_t> *indices) {
    RowIndexBuilder builder(in_array, null_conversion_policy, indices);
    return builder();
  }

  virtual ~RowIndexBuilder() = default;

  // Handle NullType explicitly since process() accesses Value() method
  // which isn't present for arrow::NullArray.
  virtual arrow::Status Visit(const arrow::NullType &type) {
    arrow::Status status;

    if (null_conversion_policy_ < 0) {
      status = arrow::Status::Invalid(
          "Indicator arrays may not contain null values with the current "
          "arguments. Modify the null handling argument if you expect "
          "your indicator array to contain nulls");
      return status;
    } else if (null_conversion_policy_ > 0) {
      if (indicator_array_->null_count() != indicator_array_->length()) {
        status = arrow::Status::UnknownError(
            "Indicator array is of type arrow::NullArray of length: ",
            indicator_array_->length(), " but contains ",
            indicator_array_->null_count(), " null values.");
        return status;
      }
      indices_->resize(indicator_array_->length());
      for (size_t i = 0; i < indices_->size(); ++i) {
        (*indices_)[i] = i;
      }
    }
    return status;
  }

  ROW_INDEX_BUILDER_VISITOR(arrow::BooleanType)
  ROW_INDEX_BUILDER_VISITOR(arrow::Int8Type)
  ROW_INDEX_BUILDER_VISITOR(arrow::Int16Type)
  ROW_INDEX_BUILDER_VISITOR(arrow::Int32Type)
  ROW_INDEX_BUILDER_VISITOR(arrow::Int64Type)
  ROW_INDEX_BUILDER_VISITOR(arrow::UInt8Type)
  ROW_INDEX_BUILDER_VISITOR(arrow::UInt16Type)
  ROW_INDEX_BUILDER_VISITOR(arrow::UInt32Type)
  ROW_INDEX_BUILDER_VISITOR(arrow::UInt64Type)

  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::HalfFloatType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::FloatType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::DoubleType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::StringType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::BinaryType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::FixedSizeBinaryType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::Decimal128Type)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::Date32Type)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::Date64Type)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::Time32Type)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::Time64Type)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::TimestampType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::ListType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::DictionaryType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::StructType)
  ROW_INDEX_BUILDER_VISITOR_NOT_IMPL(arrow::UnionType)

private:
  const std::shared_ptr<arrow::ChunkedArray> &indicator_array_;
  int null_conversion_policy_;
  std::vector<int64_t> *indices_;

  // Create a row index builder
  RowIndexBuilder(const std::shared_ptr<arrow::ChunkedArray> &in_array,
                  int null_conversion_policy, std::vector<int64_t> *indices)
      : TypeVisitor(), indicator_array_(in_array),
        null_conversion_policy_(null_conversion_policy), indices_(indices) {}

  // Build the index vector
  arrow::Status operator()() { return indicator_array_->type()->Accept(this); }

  // The virtual Visit function calls this helper function providing the
  // concrete ArrayType. This function loops over chunks adding indices to
  // the vector.
  template <typename ArrayType> arrow::Status process() {
    arrow::Status status;

    // reserve space in our indicator
    indices_->reserve(indicator_array_->length());

    // Prepare a vector that holds all the ones in the indicator to decide
    // between slice vs copy the underlying column values.
    // NOTE: We can also perform this processing per indicator chunk if the
    // total
    // entries exceed what can be represented by `size_t`.
    int64_t chunk_offset = 0;
    for (int64_t chunk_idx = 0; chunk_idx < indicator_array_->num_chunks();
         ++chunk_idx) {
      // For each chunk:
      // Cast to concrete array and exit if fail.
      // Loop over chunk elements and stores positions of ones.
      std::shared_ptr<ArrayType> array = std::dynamic_pointer_cast<ArrayType>(
          indicator_array_->chunk(chunk_idx));
      if (!array) {
        status = arrow::Status::NotImplemented(
            "Indicator type (", array->type()->ToString(),
            ") is not supported. Underlying array type for indicator must be a "
            "BooleanArray or a NumericArray of integer type.");
        return status;
      }

      const ArrayType &arr = *array;
      for (int64_t array_idx = 0; array_idx < arr.length(); ++array_idx) {
        if (arr.IsNull(array_idx)) {
          if (null_conversion_policy_ < 0) {
            status = arrow::Status::Invalid(
                "Indicator arrays may not contain null values with the current "
                "arguments. Modify the null handling argument if you expect "
                "your indicator array to contain nulls");
            return status;
          } else if (null_conversion_policy_ > 0) {
            indices_->push_back(array_idx + chunk_offset);
          }
        } else {
          // both integer and boolean arrays we can compare against 0.
          const bool is_selected = arr.Value(array_idx) != 0;
          if (is_selected) {
            indices_->push_back(array_idx + chunk_offset);
          }
        }
      }
      chunk_offset += arr.length();
    }
    return status;
  }
};
}

#endif /* ARROWOPS_INDICATOR_H */
