from metaflow.exception import MetaflowException


class MetaflowTableException(MetaflowException):
    headline = "Table error"


class MetaflowTableNotFound(MetaflowException):
    headline = "Table not found"


class MetaflowTableExists(MetaflowException):
    headline = "Table already exists"


class MetaflowTableConflict(MetaflowException):
    headline = "Conflict in table"


class MetaflowTableIncompatible(MetaflowException):
    headline = "Incompatible table"


class MetaflowTableTypeMismatch(MetaflowException):
    headline = "Invalid column type"


class MetaflowTableEmptyPartition(MetaflowException):
    headline = "Empty partition"


class MetaflowTableInvalidData(MetaflowException):
    headline = "Invalid data written to table"


class MetaflowTableEmptyTable(MetaflowException):
    headline = "Empty table"


class MetaflowTableRateLimitException(MetaflowException):
    headline = "Rate limit exceeded"


class MetaflowTableCommitStateUnknownException(MetaflowException):
    headline = "Unknown commit state"

    # For reference, see the Iceberg Java code:
    # https://github.com/apache/iceberg/blob/61e8acecf512d8dd2a72727803e5836ea1099eed/api/src/main/java/org/apache/iceberg/exceptions/CommitStateUnknownException.java#L28-L33
    msg = """
Cannot determine whether the commit was successful or not, the underlying data files
may or may not be needed. Manual intervention via the Remove Orphan Files Action can
remove these files when a connection to the Catalog can be re-established if the
commit was actually unsuccessful.
Please check to see whether or not your commit was successful before retrying this
commit. Retrying an already successful operation will result in duplicate records
or unintentional modifications.
At this time no files will be deleted including possibly unused manifest lists.
    """

    def __init__(self):
        super().__init__(self.msg)
