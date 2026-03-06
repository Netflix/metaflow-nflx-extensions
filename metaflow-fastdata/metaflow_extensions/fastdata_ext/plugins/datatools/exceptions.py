from metaflow.exception import MetaflowException


# parquet exceptions
class ParquetDecodingException(MetaflowException):
    headline = "Parquet decoding failed"


class ParquetEncodingException(MetaflowException):
    headline = "Parquet encoding failed"


# dataframe exceptions
class MetaflowDataFrameException(MetaflowException):
    headline = "Metaflow dataframe operation failed"


class MetaflowDataFrameUnknownType(MetaflowException):
    headline = "Unknown column type in Metaflow dataframe"


class MetaflowDataFrameBufferException(MetaflowException):
    headline = "Metaflow dataframe buffer operation failed"


class MetaflowDataFrameBufferNotLargeEnough(MetaflowException):
    headline = "The buffer size provided is not large enough to write this dataframe"


class MetaflowDataFrameBufferEmptyDataFrame(MetaflowException):
    headline = "Empty dataframes can not be written from buffer"


class MetaflowDataFrameInvalidBuffer(MetaflowException):
    headline = "Invalid buffer provided to read dataframe"


# iceberg exceptions
class MetaflowIcebergManifestException(MetaflowException):
    headline = "Metaflow iceberg manifest error"


class MetaflowIcebergMetadataException(MetaflowException):
    headline = "Metaflow iceberg metadata error"


class MetaflowIcebergDuplicateDataFiles(MetaflowException):
    headline = "Duplicate data files"


# cache exceptions
class CacheError(MetaflowException):
    headline = "Cache operation failed"


class CacheNotFound(MetaflowException):
    headline = "cache data not found"
