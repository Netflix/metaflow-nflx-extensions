class MetaflowFunctionException(Exception):
    headline = "Metaflow function error"

    def __init__(self, msg="", lineno=None, source_file=None):
        self.message = msg
        self.line_no = lineno
        self.source_file = source_file
        super().__init__(msg)

    def __str__(self):
        prefix = self.headline
        if self.message:
            return "%s: %s" % (prefix, self.message)
        return prefix


class MetaflowFunctionArgumentsException(MetaflowFunctionException):
    headline = "Error with Metaflow functions arguments"


class MetaflowFunctionTypeException(MetaflowFunctionException):
    headline = "Error with Metaflow functions types"


class MetaflowFunctionReturnTypeException(MetaflowFunctionException):
    headline = "Error with Metaflow function return type"


class MetaflowFunctionMemoryException(MetaflowFunctionException):
    headline = "Metaflow function memory error"


class MetaflowFunctionMemorySignalException(MetaflowFunctionException):
    headline = "Metaflow function memory signal error"


class MetaflowFunctionRuntimeException(MetaflowFunctionException):
    headline = "Metaflow function runtime error"


class MetaflowFunctionSerializationException(MetaflowFunctionException):
    headline = "Metaflow function serialization error"


class MetaflowFunctionRegistrationException(MetaflowFunctionException):
    headline = "Metaflow function registration error"


class MetaflowFunctionWatchdogException(MetaflowFunctionException):
    headline = "Metaflow function watchdog error"


class MetaflowFunctionSchemaException(MetaflowFunctionException):
    headline = "Metaflow function schema error"


class MetaflowFunctionIOException(MetaflowFunctionException):
    headline = "Metaflow function proxy error"


class MetaflowFunctionUserException(MetaflowFunctionException):
    headline = "Metaflow function user error"
