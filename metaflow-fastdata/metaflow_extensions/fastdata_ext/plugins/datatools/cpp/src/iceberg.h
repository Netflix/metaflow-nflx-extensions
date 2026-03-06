#ifndef METAFLOW_DATA_ICEBERG
#define METAFLOW_DATA_ICEBERG

#ifdef __cplusplus
extern "C" {
#endif

/*
################################################################################
Public functions
################################################################################
*/

/*
  Convert an iceberg manifest list or manifest file to json.

  filename: utf-8 encoded string
  type: "MANIFEST_LIST" or "MANIFEST_FILE"
  metadata: Metadata string
  json_str: Pointer to an array of json strings
  size: Size of the json string array
  exception: An exception string
*/
void md_iceberg_to_json(const char* filename,
                        const char* type,
                        char** metadata,
                        char*** json_str,
                        int64_t* size,
                        char* exception);

#ifdef __cplusplus
}
#endif

#endif /* METAFLOW_DATA_ICEBERG */
