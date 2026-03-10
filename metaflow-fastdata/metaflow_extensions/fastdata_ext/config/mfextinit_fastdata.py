from metaflow.metaflow_config_funcs import from_conf

###
# Default configuration
###
# Use boto3 for S3 access (OSS default; Netflix overrides this with bdp_boto)
DEFAULT_AWS_CLIENT_PROVIDER = from_conf("DEFAULT_AWS_CLIENT_PROVIDER", "boto3")

# Default metadata catalog implementation (OSS: hive_thrift; Netflix: metacat)
DEFAULT_METADATA_CATALOG = from_conf("DEFAULT_METADATA_CATALOG", "hive_thrift")

# Hive Metastore URI for the HiveThriftCatalog
METAFLOW_HIVE_METASTORE_URI = from_conf(
    "METAFLOW_HIVE_METASTORE_URI", "thrift://localhost:9083"
)


###
# Override pinned conda libraries to include CFFI.
###
def get_pinned_conda_libs(python_version, datastore_type):
    return {
        "pyarrow": ">=0.17.1",
        "pandas": ">=0.24.0",
        "cffi": ">=1.13.0,!=1.15.0",
        "fastavro": ">=1.6.0",
    }


###
# Debug options
###
DEBUG_OPTIONS = ["table", "functions"]
