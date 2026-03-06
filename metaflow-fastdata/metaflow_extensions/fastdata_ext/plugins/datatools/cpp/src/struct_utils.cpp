#include "struct_utils.h"

#include <sstream>

#include "arrow/array/util.h"

#include "log.h"
#include "util.h"

namespace {
std::vector<std::shared_ptr<arrow::Field>>
extract_fields(std::shared_ptr<arrow::Field> list_field,
               const char **child_columns, int num_child_columns,
               bool promote_single_column) {
  std::shared_ptr<arrow::BaseListType> list_type =
      std::dynamic_pointer_cast<arrow::BaseListType>(list_field->type());
  std::shared_ptr<arrow::Field> struct_field = list_type->value_field();
  std::shared_ptr<arrow::StructType> struct_type =
      std::dynamic_pointer_cast<arrow::StructType>(struct_field->type());

  std::vector<std::shared_ptr<arrow::Field>> selected_fields;
  for (int64_t i = 0; i < num_child_columns; ++i) {
    std::string name(child_columns[i]);
    selected_fields.push_back(struct_type->GetFieldByName(name));
  }

  if (std::any_of(selected_fields.begin(), selected_fields.end(),
                  [](const std::shared_ptr<arrow::Field> &p) {
                    return p == nullptr;
                  })) {
    std::vector<std::string> missing_fields;
    for (int64_t i = 0; i < num_child_columns; ++i) {
      if (selected_fields[i] == nullptr) {
        missing_fields.emplace_back(child_columns[i]);
      }
    }
    std::vector<std::string> field_names;
    for (const auto &field : struct_type->fields()) {
      field_names.push_back(field->name());
    }
    std::stringstream ss;
    ss << "Requesting non-existant fields in project_struct_list: ("
       << ops::join_str(missing_fields, ",") << "), available fields: ("
       << ops::join_str(field_names, ",") << ")";
    throw std::runtime_error(ss.str());
  }

  return selected_fields;
}

// Return an arrow array whose validity bitmap is the union of the inner
// array's validity bitmap and the parent's.
std::shared_ptr<arrow::Array>
merge_validity(std::shared_ptr<arrow::Array> array,
               const arrow::StructArray &parent) {
  if (parent.null_count() == 0) {
    return array;
  } else if (array->null_count() == 0) {
    // Copy parent validity buffer over to child
    auto array_data = array->data()->Copy();
    auto buffers = array_data->buffers;
    buffers[0] = parent.null_bitmap();
    return arrow::MakeArray(std::make_shared<arrow::ArrayData>(
        array_data->type, array_data->length, buffers, array_data->child_data,
        array_data->null_count, array_data->offset));
  } else {
    // Merge parent validity buffer
    auto array_data = array->data()->Copy();
    auto buffers = array_data->buffers;
    auto child_validity = buffers[0];
    auto parent_validity = parent.null_bitmap();
    auto merged_buffer =
        ops::unwrap(arrow::AllocateBuffer(parent_validity->size()));
    for (int64_t i = 0; i < parent_validity->size(); ++i) {
      merged_buffer->mutable_data()[i] =
          child_validity->data()[i] & parent_validity->data()[i];
    }
    buffers[0] = std::shared_ptr<arrow::Buffer>(merged_buffer.release());
    return arrow::MakeArray(std::make_shared<arrow::ArrayData>(
        array_data->type, array_data->length, buffers, array_data->child_data,
        array_data->null_count, array_data->offset));
  }
}

// Build the projected schema from top-level schema and args.
std::shared_ptr<arrow::Field> build_projected_field(
    std::shared_ptr<arrow::Field> list_field,
    const std::vector<std::shared_ptr<arrow::Field>> selected_fields,
    bool promote_single_column) {
  std::shared_ptr<arrow::BaseListType> list_type =
      std::dynamic_pointer_cast<arrow::BaseListType>(list_field->type());
  std::shared_ptr<arrow::Field> struct_field = list_type->value_field();

  // Construct child field: either struct or struct-subfield
  std::string child_name;
  std::shared_ptr<arrow::Field> child_field;
  if (selected_fields.size() == 1 && promote_single_column) {
    child_field = selected_fields.front();
    child_name = child_field->name();
  } else {
    child_name = struct_field->name();
    child_field = std::make_shared<arrow::Field>(
        child_name, std::make_shared<arrow::StructType>(selected_fields));
  }

  // Reconstruct list
  std::shared_ptr<arrow::BaseListType> projected_list;
  if (auto large_list =
          std::dynamic_pointer_cast<arrow::LargeListType>(list_type)) {
    projected_list = std::make_shared<arrow::LargeListType>(child_field);
  } else {
    projected_list = std::make_shared<arrow::ListType>(child_field);
  }

  return std::make_shared<arrow::Field>(list_field->name(), projected_list);
}
} // namespace

namespace ops {
std::unique_ptr<CMetaflowChunkedArray>
project_struct_list_array(struct CMetaflowChunkedArray *array,
                          const char **child_columns, int num_child_columns,
                          bool promote_single_column) {
  if (promote_single_column && num_child_columns != 1) {
    LOG << "Requesting to promote multiple columns in struct list projection";
  }
  const bool promote_column = promote_single_column && num_child_columns == 1;

  auto &chunked_array = array->chunked_array;

  // Determine child fields
  auto selected_fields = extract_fields(
      array->schema, child_columns, num_child_columns, promote_single_column);

  // Build projected structs
  arrow::ArrayVector chunks;
  for (auto i = 0; i < chunked_array->num_chunks(); ++i) {
    auto chunk = chunked_array->chunk(i);
    // Extract child struct
    std::unique_ptr<arrow::StructArray> values_struct;
    if (auto large_list =
            std::dynamic_pointer_cast<arrow::LargeListArray>(chunk)) {
      auto values = large_list->values();
      values_struct = ops::make_unique<arrow::StructArray>(values->data());
    } else {
      auto list_chunk = std::dynamic_pointer_cast<arrow::ListArray>(chunk);
      auto values = list_chunk->values();
      values_struct = ops::make_unique<arrow::StructArray>(values->data());
    }

    // Extract struct fields
    arrow::ArrayVector selected_children;
    arrow::FieldVector fields;
    for (int i = 0; i < num_child_columns; ++i) {
      std::string name(child_columns[i]);
      auto child_array = values_struct->GetFieldByName(name);
      fields.push_back(
          std::make_shared<arrow::Field>(name, child_array->type()));
      selected_children.push_back(child_array);
    }

    // Construct projected array
    std::shared_ptr<arrow::Array> projected_child =
        promote_column ? merge_validity(selected_children[0], *values_struct)
                       : ops::unwrap(arrow::StructArray::Make(selected_children,
                                                              selected_fields));
    // Construct list of projected array
    if (auto large_list =
            std::dynamic_pointer_cast<arrow::LargeListArray>(chunk)) {
      auto projected_list = ops::unwrap(arrow::LargeListArray::FromArrays(
          *large_list->offsets(), *projected_child));
      chunks.push_back(projected_list);
    } else {
      auto list_chunk = std::dynamic_pointer_cast<arrow::ListArray>(chunk);
      auto projected_list = ops::unwrap(arrow::ListArray::FromArrays(
          *list_chunk->offsets(), *projected_child));
      chunks.push_back(projected_list);
    }
  }

  // Convert to CMetaflowChunkedArray
  auto child_chunked_array =
      std::make_shared<arrow::ChunkedArray>(std::move(chunks));
  auto child_field = build_projected_field(array->schema, selected_fields,
                                           promote_single_column);
  return ops::make_unique<CMetaflowChunkedArray>(std::move(child_chunked_array),
                                                 std::move(child_field));
}

} // namespace ops