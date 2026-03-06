import uuid

# Generate a unique session ID for lineage tracking
BDP_LINEAGE_SESSION_ID = uuid.uuid4().hex

# table package public API
from .table import Table
from .exception import (
    MetaflowTableException,
    MetaflowTableNotFound,
    MetaflowTableIncompatible,
    MetaflowTableTypeMismatch,
    MetaflowTableEmptyPartition,
)
from .catalog import MetadataCatalog, TableInfo
