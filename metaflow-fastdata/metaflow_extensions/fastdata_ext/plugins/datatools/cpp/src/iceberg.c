#include "avro.h"
#include "clog.h"
#include "iceberg.h"

#define NULL_IDX 0
#define INIT_ARRAY_CAPACITY 256
#define INIT_STRING_CAPACITY 512
#define INIT_META_CAPACITY 512

#define RETURN_IF_ERROR(x) if (x != 0) return -1

/*
  Append a string to an array of strings, extending its size as needed.
   The returned array must be free'd.

  returns: A pointer to a string (possibly the same one that was passed in)
*/
char **append_array(char *value, char **src, int64_t *size, int64_t *max_size) {
  char **dest = NULL;
  int64_t new_size = *size + 1;
  if ((new_size > *max_size) || (src == NULL)) {
    if (src == NULL) {
      *max_size = (new_size > *max_size) ? new_size : *max_size;
      dest = (char **)malloc(sizeof(char *) * (*max_size));
      *size = 0;
    } else {
      *max_size = (*max_size) * 2;
      dest = (char **)malloc(sizeof(char *) * (*max_size));
      memcpy(dest, src, sizeof(char *) * (*size));
      free(src);
    }
  } else {
    dest = src;
  }
  dest[*size] = value;
  *size = new_size;
  return dest;
}

/*
  Append chars to a string, extending its size as needed.

  The returned array must be free'd.

  value: String to be appended
  src: Source string
  size: Current length of source string
  max_size: source string character buffer length

  The returned array must be free'd.

  returns: A pointer to a string (possibly the same one that was passed in)

  This function may allocate memory on the caller's behalf. This function may
  return the same pointer as 'src' if src is big enough to add the contents of
  value. If it isn't new memory will be allocated and 'src' will be free'd. A
  common usage is:

  char* str = NULL;
  int64_t size = 0;
  int64_t max = 0; // set larger to have a bigger initial buffer

  str = append_string("a string to append", -1, str, &size, &max)
  str = append_string(", and another string to append", -1, str, &size, &max)

  The returned array must be free'd by the caller.
*/
char *append_string(const char *value, int64_t value_size, char *src,
                    int64_t *size, int64_t *max_size) {
  if (value_size < 0)
    value_size = strlen(value);

  if (*size < 0)
    *size = strlen(src);

  char *dest = NULL;
  int64_t new_size = *size + value_size;
  if ((new_size > *max_size) || (src == NULL)) {
    while (*max_size < new_size)
      *max_size = *max_size * 2;
    dest = (char *)malloc(*max_size + 1);
    if (src != NULL) {
      memcpy(dest, src, *size);
      free(src);
    }
  } else {
    dest = src;
  }
  memcpy(dest + (*size), value, value_size);
  *size = *size + value_size;
  dest[*size] = '\0';
  return dest;
}

/*
 Convert bytes to an ASCII Hex string.

 src: Pointer to the source bytes
 src_len: The length of the source bytes
 dest: Pointer to destination string
 dest_len: The length of the destination bytes
   The 'dest' array must be free'd.
*/
int bytes_to_hex_json(const void *src, int64_t src_len, char **dest,
                      int64_t *dest_len) {
  *dest_len = src_len * 2 + 2;           //+2 for the quotes
  *dest = (char *)malloc(*dest_len + 1); //+1 for terminator
  if (*dest == NULL)
    return -1;
  for (int64_t i = 0; i < src_len; ++i) {
    sprintf(*dest + 2 * i + 1, "%02x", ((unsigned char *)src)[i]);
  }
  (*dest)[0] = '"';
  (*dest)[*dest_len - 1] = '"';
  (*dest)[*dest_len] = '\0';
  return 0;
}

/*
  Get avro types as string for debugging.
*/
char* get_type_as_string(avro_value_t * value) {
  avro_type_t type = avro_value_get_type(value);
  switch(type) {
  case AVRO_STRING:
    return "STRING";
  case AVRO_BYTES:
    return "BYTES";
  case AVRO_INT32:
    return "INT32";
  case AVRO_INT64:
    return "INT64";
  case AVRO_FLOAT:
    return "FLOAT";
  case AVRO_DOUBLE:
    return "DOUBLE";
  case AVRO_BOOLEAN:
    return "BOOLEAN";
  case AVRO_NULL:
    return "NULL";
  case AVRO_ENUM:
    return "ENUM";
  case AVRO_FIXED:
    return "FIXED";
  case AVRO_MAP:
    return "MAP";
  case AVRO_LINK:
    return "LINK";
  case AVRO_RECORD:
    return "RECORD";
  case AVRO_ARRAY:
    return "ARRAY";
  case AVRO_UNION:
    return "UNION";
  default:
    return "";
  }
}

/*
  Convert a value to json with custom handling of bytes and union types

  The destination string should be freed with 'free' even on error.

  value: The value to convert
  dest: Character buffer to fill with results
  dest_size: Size of character buffer

  returns: 0 if success and -1 on error.
*/
int value_to_json(avro_value_t* value, char** dest, int64_t* dest_size) {
  avro_type_t type = avro_value_get_type(value);
  switch(type) {
  /*
    Encode bytes into hex instead of unicode, which guarantees its ASCII and
    makes it easy to convert to python bytes transparently.
  */
  case AVRO_BYTES: {
    const void* val = NULL;// this is a weak pointer to avro data, we won't
                           // need to free it.
    size_t size = 0;
    RETURN_IF_ERROR(avro_value_get_bytes(value, &val, &size));
    RETURN_IF_ERROR(bytes_to_hex_json(val, size, dest, dest_size));
    return 0;
  }
  /*
      Union's are used for optional values in iceberg, where the union of
      type [Null, <Type>]. Handle this case here.
  */
  case AVRO_UNION: {
    int discriminant;
    avro_value_t branch;
    RETURN_IF_ERROR(avro_value_get_discriminant(value, &discriminant));

    // its a null
    if (discriminant == NULL_IDX)
    {
      *dest_size = 4; // length of 'null' + terminator
      *dest = malloc(*dest_size+1);
      strcpy(*dest, "null");
      return 0;
    }

    // Get the branch and code its value to json
    RETURN_IF_ERROR(avro_value_get_current_branch(value, &branch));
    RETURN_IF_ERROR(value_to_json(&branch, dest, dest_size));
    return 0;
  }
  /*
    Use the default value to json function
  */
  default: {
    RETURN_IF_ERROR(avro_value_to_json(value, 1, dest));
    *dest_size = strlen(*dest);
    return 0;
  }
  }
  return 0;
}

/*
  Write schema to a string

  schema: avro schema
  line: Character buffer to insert line
  line_size: Current position into the character buffer
  max_line_size: Current max size of the character buffer
  exception: exception string

  Returns 0 if success and -1 on error.
*/
int schema_to_json(avro_schema_t* schema,
                   char** line,
                   char* exception) {
  avro_writer_t writer = avro_writer_memory(*line, INIT_META_CAPACITY);
  if (avro_schema_to_json(*schema, writer)) {
    sprintf(exception, "Could not decode metadata. %s", avro_strerror());
    return -1;
  }

  avro_write(writer, (void *)"", 1);
  avro_writer_free(writer);
  return 0;
}

/*
  Get a value from a record as a string and insert it into a character buffer.

  record: avro value to insert
  field: name of field as null terminated string
  as_json: Insert as a json value
  line: Character buffer to insert line
  line_size: Current position into the character buffer
  max_line_size: Current max size of the character buffer
  exception: exception string

  Returns 0 if success and -1 on error.
*/
int insert_value(avro_value_t* record,
                 const char* field,
                 int as_json,
                 char** line,
                 int64_t* line_size,
                 int64_t* max_line_size,
                 char* exception) {
  avro_value_t value;
  char* json_value = NULL;
  int64_t json_value_size = 0;

  if (avro_value_get_by_name(record,
                             field,
                             &value,
                             NULL) != 0) {
    sprintf(exception,
            "The field '%s' cannot be read from the avro file.", field);
    return -1;
  }
  if (value_to_json(&value, &json_value, &json_value_size) != 0) {
    sprintf(exception,
            "The field '%s' cannot be converted to JSON.", field);
    return -1;
  }
  if (as_json) {
    *line = append_string("\"", -1,
                          *line, line_size, max_line_size);
    *line = append_string(field, -1,
                          *line, line_size, max_line_size);
    *line = append_string("\":", -1,
                          *line, line_size, max_line_size);
  }
  *line = append_string(json_value, json_value_size,
                        *line, line_size, max_line_size);

  free(json_value);
  return 0;
}

int metadata_to_json(const char* filename, char** metadata, char* exception){
  int rval;
  avro_schema_t meta_schema;
  avro_schema_t meta_values_schema;
  avro_value_iface_t *meta_iface;
  avro_value_t meta;
  char magic[4];

  // open the file
  FILE * fp = fopen(filename, "r");
  if (fp == NULL) {
    sprintf(exception,
            "The file does not exist or is "\
            "not a valid avro file");
    return 1;
  }

  // check valid avro file
  avro_reader_t reader = avro_reader_file(fp);
  avro_read(reader, magic, sizeof(magic));
  if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j'
      || magic[3] != 1) {
    sprintf(exception,
            "The file does not exist or is "\
            "not a valid avro file");
    return 1;
  }

  // read metadata
  meta_values_schema = avro_schema_bytes();
  meta_schema = avro_schema_map(meta_values_schema);
  meta_iface = avro_generic_class_from_schema(meta_schema);
  if (meta_iface == NULL) {
    sprintf(exception, "Error reading file header");
    return 1;
  }
  avro_generic_value_new(meta_iface, &meta);
  rval = avro_value_read(reader, &meta);
  if (rval) {
    sprintf(exception, "Error reading file header");
    return 1;
  }

  // convert to json
  int64_t metadata_size = 0;
  rval = value_to_json(&meta, metadata, &metadata_size);
  if (rval) {
    sprintf(exception, "Error reading file header");
    return 1;
  }

  // free objects
  fclose(fp);
  avro_schema_decref(meta_schema);
  avro_schema_decref(meta_values_schema);
  avro_value_iface_decref(meta_iface);
  avro_value_decref(&meta);

  return 0;
}

/*
  Convert a iceberg manifest list to json

  filename: Null terminated utf-8 string
  metadata: pointer to array of characters
  json_str: pointer to a array of characters
  size: pointer to an int for the allocated size of the character array
  exception: Charcter buffer for expections

  Note: json_str will be allocated by the function and must be fee'd by the
  caller. The exception buffer must be pre-allocated.
*/
void iceberg_manifest_list_to_json(const char* filename,
                                   char** metadata,
                                   char*** json_str,
                                   int64_t* size,
                                   char* exception) {
  // Reset exception buffer.
  if (exception != NULL)
    exception[0] = '\0';

  // try reading header
  if (metadata_to_json(filename, metadata, exception))
    return;

  // Instantiate a reader.
  avro_file_reader_t dbreader;
  if (avro_file_reader(filename, &dbreader)) {
    sprintf(exception,
            "The file does not exist or is "\
            "not a valid avro file");
    return;
  }

  // Initialize our array.
  *json_str = NULL;
  *size = 0;
  int64_t max_array_size = INIT_ARRAY_CAPACITY;

  // Get a reader schema from the writer.
  avro_schema_t schema;
  schema = avro_file_reader_get_writer_schema(dbreader);

  // Create a record.
  avro_value_t record;
  avro_value_iface_t *record_class = avro_generic_class_from_schema(schema);
  avro_generic_value_new(record_class, &record);

  // Loop over records.
  while (avro_file_reader_read_value(dbreader, &record) == 0) {
    avro_value_t value;
    int discriminant;
    avro_value_t part_array;

    char* line = NULL;
    int64_t line_size = 0;
    int64_t max_line_size = INIT_STRING_CAPACITY;

    line = append_string("{", -1,
                         line, &line_size, &max_line_size);
    if (insert_value(&record,
                     "manifest_path",
                     1,
                     &line,
                     &line_size,
                     &max_line_size,
                     exception) != 0)
      return;

    line = append_string(",", -1,
                         line, &line_size, &max_line_size);
    if (insert_value(&record,
                     "partition_spec_id",
                     1,
                     &line,
                     &line_size,
                     &max_line_size,
                     exception) != 0)
      return;

    // Read bounds arrays
    if (avro_value_get_by_name(&record,
                               "partitions",
                               &value,
                               NULL) != 0) {
      sprintf(exception,
              "The field 'partitions' "\
              "cannot be read from the avro file.");
      return;
    }

    // Check and fail if null
    if (avro_value_get_discriminant(&value, &discriminant) != 0) {
      sprintf(exception,
              "The field 'partitions' "\
              "is of an invalid type.");
      return;
    }
    if (discriminant == NULL_IDX)
    {
      sprintf(exception,
              "The field 'partitions' "\
              "is required in this implementation.");
      return;
    }

    // Get the non-null branch of the union
    if (avro_value_get_current_branch(&value, &part_array) != 0) {
      sprintf(exception,
              "Cannot read the value of field 'partitions'.");
      return;
    }

    // Get the array size
    size_t parts;
    if (avro_value_get_size(&part_array, &parts) != 0) {
      sprintf(exception,
              "Cannot read the number of partitions.");
      return;
    }

    // Looper over elements
    line = append_string(",\"partitions\": [", -1,
                         line, &line_size, &max_line_size);
    for (size_t i = 0; i < parts; ++i) {
      avro_value_t part_value;
      if (avro_value_get_by_index(&part_array, i, &part_value, NULL) != 0) {
        sprintf(exception,
                "Cannot read partitions bounds.");
        return;
      }

      if (i > 0) {
        line = append_string(",[", -1,
                             line, &line_size, &max_line_size);
      }
      else {
        line = append_string("[", -1,
                             line, &line_size, &max_line_size);
      }
      if (insert_value(&part_value,
                       "contains_null",
                       0,
                       &line,
                       &line_size,
                       &max_line_size,
                       exception) != 0)
        return;

      line = append_string(",", -1,
                           line, &line_size, &max_line_size);
      if (insert_value(&part_value,
                       "lower_bound",
                       0,
                       &line,
                       &line_size,
                       &max_line_size,
                       exception) != 0)
        return;

      line = append_string(",", -1,
                           line, &line_size, &max_line_size);
      if (insert_value(&part_value,
                       "upper_bound",
                       0,
                       &line,
                       &line_size,
                       &max_line_size,
                       exception) != 0)
        return;

      line = append_string("]", -1,
                           line, &line_size, &max_line_size);
    }

    // Close the JSON stanza
    line = append_string("]}", -1, line, &line_size, &max_line_size);
    line[line_size]='\0';

    // Append this line to list of lines
    *json_str = append_array(line, *json_str, size, &max_array_size);
  }

  // Free memory
  avro_file_reader_close(dbreader);
  avro_schema_decref(schema);
  avro_value_decref(&record);
  avro_value_iface_decref(record_class);
}

/*
  Convert an iceberg manifest file

  filename: Null terminated utf-8 string
  metadata: point to array of chars for metadata
  json_str: pointer to a array of characters
  size: pointer to an int for the allocated size of the character array
  exception: Charcter buffer for expections

  Note: json_str will be allocated by the function and must be fee'd by the
  caller. The exception buffer must be pre-allocated.
*/
void iceberg_manifest_file_to_json(const char* filename,
                                   char** metadata,
                                   char*** json_str,
                                   int64_t* size,
                                   char* exception) {
  // Reset exception buffer.
  if (exception != NULL)
    exception[0] = '\0';

  // try reading header
  if (metadata_to_json(filename, metadata, exception))
    return;

  // Instantiate a reader.
  avro_file_reader_t dbreader;
  if (avro_file_reader(filename, &dbreader)) {
    sprintf(exception,
            "The file does not exist or is "\
            "not a valid avro file. %s",
            avro_strerror());
    return;
  }

  // Initialize our array.
  *json_str = NULL;
  *size = 0;
  int64_t max_array_size = INIT_ARRAY_CAPACITY;

  // Get a reader schema from the writer.
  avro_schema_t schema;
  schema = avro_file_reader_get_writer_schema(dbreader);

  // Create a record.
  avro_value_t record;
  avro_value_iface_t *record_class = avro_generic_class_from_schema(schema);
  avro_generic_value_new(record_class, &record);

  // Loop over records.
  while (avro_file_reader_read_value(dbreader, &record) == 0) {
    avro_value_t value;
    char* line = NULL;
    int64_t line_size = 0;
    int64_t max_line_size = INIT_STRING_CAPACITY;

    // Entry status
    line = append_string("{", -1,
                         line, &line_size, &max_line_size);
    if (insert_value(&record,
                     "status",
                     1,
                     &line,
                     &line_size,
                     &max_line_size,
                     exception) != 0)
      return;

    // data_file struct
    if (avro_value_get_by_name(&record,
                               "data_file",
                               &value,
                               NULL) != 0) {
      sprintf(exception,
              "The field 'data_file' "\
              "cannot be read from the avro file.");
      return;
    }

    // file format
    line = append_string(",", -1,
                         line, &line_size, &max_line_size);
    if (insert_value(&value,
                     "file_format",
                     1,
                     &line,
                     &line_size,
                     &max_line_size,
                     exception) != 0)
      return;

    // file path
    line = append_string(",", -1,
                         line, &line_size, &max_line_size);
    if (insert_value(&value,
                     "file_path",
                     1,
                     &line,
                     &line_size,
                     &max_line_size,
                     exception) != 0)
      return;

    // record counts
    line = append_string(",", -1,
                         line, &line_size, &max_line_size);
    if (insert_value(&value,
                     "record_count",
                     1,
                     &line,
                     &line_size,
                     &max_line_size,
                     exception) != 0)
      return;

    // file size in bytes
    line = append_string(",", -1,
                         line, &line_size, &max_line_size);
    if (insert_value(&value,
                     "file_size_in_bytes",
                     1,
                     &line,
                     &line_size,
                     &max_line_size,
                     exception) != 0)
      return;

    // Process the partition tuple
    avro_value_t partition;
    if (avro_value_get_by_name(&value,
                               "partition",
                               &partition,
                               NULL) != 0) {
      sprintf(exception,
              "The field 'partition' cannot be read from the avro file.");
      return;
    }

    // Get the number of elements in the partition tuple
    size_t parts;
    if (avro_value_get_size(&partition, &parts) != 0) {
      sprintf(exception,
              "Cannot read the number of partitions.");
      return;
    }
    // Looper over elements
    line = append_string(",\"partition\": {", -1,
                         line, &line_size, &max_line_size);
    for (size_t i = 0; i < parts; ++i) {
      avro_value_t part_value;
      const char* field = NULL;
      char* json_value = NULL;
      int64_t json_value_size = 0;

      if (avro_value_get_by_index(&partition, i, &part_value, &field) != 0) {
        sprintf(exception,
                "Cannot read partition '%s'", field);
        return;
      }
      if (value_to_json(&part_value, &json_value, &json_value_size) != 0) {
        sprintf(exception,
                "The field 'partition.%s' cannot be converted to JSON.", field);
        return;
      }

      if (i > 0) {
        line = append_string(",\"", -1,
                             line, &line_size, &max_line_size);
      }
      else {
        line = append_string("\"", -1,
                             line, &line_size, &max_line_size);
      }
      line = append_string(field, -1,
                           line, &line_size, &max_line_size);
      line = append_string("\":", -1,
                           line, &line_size, &max_line_size);
      line = append_string(json_value, json_value_size,
                           line, &line_size, &max_line_size);

      free(json_value);
    }

    // Close the JSON stanza
    line = append_string("}}", -1, line, &line_size, &max_line_size);
    line[line_size]='\0';

    // Append this line to list of lines
    *json_str = append_array(line, *json_str, size, &max_array_size);
  }

  // Free memory
  avro_file_reader_close(dbreader);
  avro_schema_decref(schema);
  avro_value_decref(&record);
  avro_value_iface_decref(record_class);
}

/*
  Convert an iceberg avro file to json

  filename: Null terminated utf-8 string
  type: Type of iceberg file "MANIFEST_LIST", "MANIFEST_FILE"
  metadata Character buffer for metadata
  json_str: pointer to a array of characters
  size: pointer to an int for the allocated size of the array of strings
  exception: Charcter buffer for expections

  Note: json_str will be allocated by the function and must be free'd by the
  caller. The exception buffer must be pre-allocated.
*/

void md_iceberg_to_json(const char* filename,
                        const char* type,
                        char** metadata,
                        char*** json_str,
                        int64_t* size,
                        char* exception) {
  if (strcmp(type, "MANIFEST_LIST") == 0) {
    iceberg_manifest_list_to_json(filename,
                                  metadata,
                                  json_str,
                                  size,
                                  exception);
  }
  else if (strcmp(type, "MANIFEST_FILE") == 0) {
    iceberg_manifest_file_to_json(filename,
                                  metadata,
                                  json_str,
                                  size,
                                  exception);
  }
  else {
    sprintf(exception,
            "'%s' isn't a valid iceberg file type",
            type);
    return;
  }
}
