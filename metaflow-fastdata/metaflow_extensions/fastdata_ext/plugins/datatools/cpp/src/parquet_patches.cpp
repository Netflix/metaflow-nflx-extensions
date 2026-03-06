#include "parquet_patches.h"

#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "arrow/util/base64.h"
#include "arrow/util/key_value_metadata.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"

#include "util.h"

namespace {

struct ListChildren {
  const parquet::schema::GroupNode *list;
  parquet::schema::NodePtr item;

  bool is_null() const { return list == nullptr; }
  static ListChildren null() { return ListChildren{nullptr, nullptr}; }
};

struct MapChildren {
  const parquet::schema::GroupNode *key_value;
  parquet::schema::NodePtr key;
  parquet::schema::NodePtr value;

  bool is_null() const { return key_value == nullptr; }
  static MapChildren null() { return MapChildren{nullptr, nullptr, nullptr}; }
};

// Check whether a GroupNode is a list instead of a Struct, and if so returns
// the "list" child and "item" or "element" skip-child nodes.  If not a list,
// returns null. In order to be a list, the group node must have structure:
//
// group $node_name (not repeated) {
//   group list (repeated) {
//     $any item
//   }
// },
// i.e. it must contain a single repeated child ("list"),
// and the "list" child must contain a single child ("item"), which
// may be of any type.
// Child nodes do not need to be named "list" or "item"
ListChildren
extract_list_children(const parquet::schema::GroupNode *group_node) {
  if (group_node->field_count() != 1) {
    return ListChildren::null();
  }
  const auto child_field = group_node->field(0);
  if (child_field->repetition() != parquet::Repetition::REPEATED) {
    return ListChildren::null();
  }
  const auto *child_group_node =
      dynamic_cast<const parquet::schema::GroupNode *>(child_field.get());
  if (child_group_node == nullptr) {
    return ListChildren::null();
  }
  if (child_group_node->field_count() != 1) {
    return ListChildren::null();
  }
  const auto item_node = child_group_node->field(0);
  return ListChildren{child_group_node, item_node};
}

// Check whether a GroupNode is a map instead of a Struct, and if so returns
// the "key_value", child and the "key" and "value" skip-child nodes.  If not
// a map, return null.  In order to be a map, the group node must have structure
// group $node_name (not repeated) {
//   group key_value (repeated) {
//     $any key
//     $any value
//   }
// }
MapChildren extract_map_children(const parquet::schema::GroupNode *group_node) {
  if (group_node->field_count() != 1) {
    return MapChildren::null();
  }
  const auto child_field = group_node->field(0);
  if (child_field->repetition() != parquet::Repetition::REPEATED) {
    return MapChildren::null();
  }
  const auto *child_group_node =
      dynamic_cast<const parquet::schema::GroupNode *>(child_field.get());
  if (child_group_node == nullptr) {
    return MapChildren::null();
  }

  if (child_group_node->field_count() != 2) {
    return MapChildren::null();
  }
  const auto key_node = child_group_node->field(0);
  const auto value_node = child_group_node->field(1);
  return MapChildren{child_group_node, key_node, value_node};
}

// Source: None
// Replace the input Node with one that has a the correct field_id assigned.
// This method will also replace all child nodes to include the correct field
// ids.
std::shared_ptr<parquet::schema::Node> assign_field_ids_recursive(
    const parquet::schema::Node *node, const std::vector<std::string> &path,
    const std::map<std::vector<std::string>, int64_t> &field_ids) {
  int64_t field_id = -1;
  if (!path.empty()) {
    auto it = field_ids.find(path);
    if (it == field_ids.end()) {
      throw std::runtime_error("No field id found for field: " +
                               ops::join_str(path, "."));
    } else {
      field_id = it->second;
    }
  }
  const auto *prim_node =
      dynamic_cast<const parquet::schema::PrimitiveNode *>(node);
  if (prim_node != nullptr) {
    const auto &decimal_metadata = prim_node->decimal_metadata();
    const auto precision =
        decimal_metadata.isset ? decimal_metadata.precision : -1;
    const auto scale = decimal_metadata.isset ? decimal_metadata.scale : -1;
    return parquet::schema::PrimitiveNode::Make(
        prim_node->name(), prim_node->repetition(), prim_node->physical_type(),
        prim_node->converted_type(), prim_node->type_length(), precision, scale,
        field_id);
  }
  const auto *group_node =
      dynamic_cast<const parquet::schema::GroupNode *>(node);
  if (group_node == nullptr) {
    throw std::runtime_error(
        "Unable to decode arrow field: " + ops::join_str(path, ".") +
        " for parquet field assignment");
  }

  // Check for list type
  if (node->converted_type() == parquet::ConvertedType::LIST) {
    const ListChildren list_children = extract_list_children(group_node);
    if (list_children.is_null()) {
      throw std::runtime_error(
          "Unable to decode list arrow field: " + ops::join_str(path, ".") +
          " for parquet field assignment");
    }

    const parquet::schema::GroupNode *list_node = list_children.list;
    const parquet::schema::NodePtr item_ptr = list_children.item;
    auto elem_path = path;
    // Field ids uses "element" for list children, while parquet uses
    // "list.item" or "list.element".
    elem_path.push_back("element");
    auto new_item_ptr =
        assign_field_ids_recursive(item_ptr.get(), elem_path, field_ids);
    auto new_list_node = parquet::schema::GroupNode::Make(
        list_node->name(), list_node->repetition(), {new_item_ptr},
        list_node->converted_type(), /* field_id= */ -1);
    return parquet::schema::GroupNode::Make(
        group_node->name(), group_node->repetition(), {new_list_node},
        group_node->converted_type(), field_id);
  }

  // Check for map type
  if (node->converted_type() == parquet::ConvertedType::MAP) {
    const MapChildren map_children = extract_map_children(group_node);
    if (map_children.is_null()) {
      throw std::runtime_error(
          "Unable to decode list arrow field: " + ops::join_str(path, ".") +
          " for parquet field assignment");
    }

    const parquet::schema::GroupNode *key_value_node = map_children.key_value;
    const parquet::schema::NodePtr key_ptr = map_children.key;
    const parquet::schema::NodePtr value_ptr = map_children.value;

    // Field ids use "key" or "value" for map children, while parquet
    // uses "key_value.key" or "key_value.value".
    auto key_path = path;
    key_path.push_back("key");
    auto new_key_ptr =
        assign_field_ids_recursive(key_ptr.get(), key_path, field_ids);
    auto value_path = path;
    value_path.push_back("value");
    auto new_value_ptr =
        assign_field_ids_recursive(value_ptr.get(), value_path, field_ids);

    auto new_key_value_node = parquet::schema::GroupNode::Make(
        key_value_node->name(), key_value_node->repetition(),
        {new_key_ptr, new_value_ptr}, key_value_node->converted_type(),
        /* field_id= */ -1);
    return parquet::schema::GroupNode::Make(
        group_node->name(), group_node->repetition(), {new_key_value_node},
        group_node->converted_type(), field_id);
  }

  // Struct type
  parquet::schema::NodeVector child_nodes;
  for (int idx = 0; idx < group_node->field_count(); ++idx) {
    auto child_node = group_node->field(idx);
    auto child_path = path;
    child_path.push_back(child_node->name());
    auto new_child_node =
        assign_field_ids_recursive(child_node.get(), child_path, field_ids);
    child_nodes.push_back(new_child_node);
  }
  return parquet::schema::GroupNode::Make(
      group_node->name(), group_node->repetition(), child_nodes,
      group_node->converted_type(), field_id);
}

// Source: None
// Replace a parquet schema with one whose nodes have the provided field_ids.
std::shared_ptr<parquet::SchemaDescriptor>
assign_field_ids(std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor,
                 const std::map<std::vector<std::string>, int64_t> &field_ids) {
  const auto *group_node = schema_descriptor->group_node();
  std::vector<std::string> path{};
  auto new_node = assign_field_ids_recursive(group_node, path, field_ids);
  auto new_descriptor = std::make_shared<parquet::SchemaDescriptor>();
  new_descriptor->Init(new_node);
  return new_descriptor;
}

// Source: arrow/result.h
// Modifications: Simplified
#define MF_ARROW_ASSIGN_OR_RAISE(lhs, rexpr)                                   \
  auto &&_result = (rexpr);                                                    \
  if (!_result.ok()) {                                                         \
    return (_result.status());                                                 \
  }                                                                            \
  lhs = std::move(_result).ValueUnsafe();

// Source: parquet/arrow/writer.cc
// Modificaitons: Added namespace and fixed call to base64_encode
arrow::Status
GetSchemaMetadata(const ::arrow::Schema &schema, ::arrow::MemoryPool *pool,
                  const parquet::ArrowWriterProperties &properties,
                  std::shared_ptr<const arrow::KeyValueMetadata> *out) {
  if (!properties.store_schema()) {
    *out = nullptr;
    return arrow::Status::OK();
  }

  static const std::string kArrowSchemaKey = "ARROW:schema";
  std::shared_ptr<arrow::KeyValueMetadata> result;
  if (schema.metadata()) {
    result = schema.metadata()->Copy();
  } else {
    result = ::arrow::key_value_metadata({}, {});
  }

  MF_ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> serialized,
                           ::arrow::ipc::SerializeSchema(schema, pool));

  // The serialized schema is not UTF-8, which is required for Thrift
  std::string schema_as_string = serialized->ToString();
  std::string schema_base64 = ::arrow::util::base64_encode(schema_as_string);
  result->Append(kArrowSchemaKey, schema_base64);
  *out = result;
  return arrow::Status::OK();
}

// Source: arrow/status.h
// Modifications: Simplified
#define MF_RETURN_NOT_OK(value)                                                \
  do {                                                                         \
    ::arrow::Status status = value;                                            \
    if (!status.ok()) {                                                        \
      return status;                                                           \
    }                                                                          \
  } while (false)

} // namespace

namespace ops {
namespace patches {
// Source: parquet/arrow/writer.cc
// Modifications: Merged FileWriter::Open into WriteTable and added call
//      to reassign field ids.  Also unpacked a PARQUET_CATCH_NOT_OK macro.
arrow::Status
WriteTable(const ::arrow::Table &table, ::arrow::MemoryPool *pool,
           std::shared_ptr<::arrow::io::OutputStream> sink, int64_t chunk_size,
           std::shared_ptr<parquet::WriterProperties> properties,
           std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties,
           const std::map<std::vector<std::string>, int64_t> *field_ids) {
  using namespace parquet;
  using namespace parquet::arrow;
  using namespace parquet::schema;
  // ---- begin code copied from parquet::arrow::WriteTable
  std::unique_ptr<parquet::arrow::FileWriter> writer;
  const auto &schema = *table.schema();

  // This is originally a call to parquet::arrow::FileWriter::Open
  // But we add a call to reassign field ids.
  // ---- begin code copied from FileWriter::Open

  std::shared_ptr<SchemaDescriptor> parquet_schema;
  MF_RETURN_NOT_OK(ToParquetSchema(&schema, *properties, *arrow_properties,
                                   &parquet_schema));

  // ---- Not in original arrow code.
  if (field_ids != nullptr) {
    parquet_schema = assign_field_ids(parquet_schema, *field_ids);
  }

  auto schema_node =
      std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());

  std::shared_ptr<const ::arrow::KeyValueMetadata> metadata;
  MF_RETURN_NOT_OK(
      GetSchemaMetadata(schema, pool, *arrow_properties, &metadata));

  std::unique_ptr<ParquetFileWriter> base_writer;
  try {
    base_writer =
        ParquetFileWriter::Open(std::move(sink), schema_node,
                                std::move(properties), std::move(metadata));
  } catch (const ::parquet::ParquetStatusException &e) {
    return e.status();
  } catch (const ::parquet::ParquetException &e) {
    return ::arrow::Status::IOError(e.what());
  }

  auto schema_ptr = std::make_shared<::arrow::Schema>(schema);
  MF_RETURN_NOT_OK(FileWriter::Make(pool, std::move(base_writer),
                                    std::move(schema_ptr),
                                    std::move(arrow_properties), &writer));

  // ---- end code copied from FileWriter::Open

  MF_RETURN_NOT_OK(writer->WriteTable(table, chunk_size));
  return writer->Close();
  // ---- end code copied from parquet::arrow::WriteTable
}
} // namespace patches
} // namespace ops

#undef MF_RETURN_NOT_OK
#undef MF_ARROW_ASSIGN_OR_RAISE
