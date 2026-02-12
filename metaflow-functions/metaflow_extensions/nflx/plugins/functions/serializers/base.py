from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Type, TypeVar

T = TypeVar("T")


class BaseSerializer(ABC):
    """
    Base class for all serializers used in MetaFlow functions.

    This provides a common interface compatible with SerializerRegistry
    for serializing and deserializing objects.
    """

    @abstractmethod
    def serialize(self, obj: Any) -> Tuple[bytes, List[Any]]:
        """
        Serialize an object to bytes.

        Parameters
        ----------
        obj : Any
            The object to serialize

        Returns
        -------
        Tuple[bytes, List[Any]]
            A tuple of (serialized_bytes, extra_data)
        """

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize bytes back to an object.

        Parameters
        ----------
        data : bytes
            The bytes to deserialize

        Returns
        -------
        Any
            The deserialized object
        """

    @property
    @abstractmethod
    def supported_type(self) -> Type:
        """
        The type this serializer supports.

        Returns
        -------
        Type
            The type class this serializer handles
        """

    def aggregator(self, results: List[Any]) -> Any:
        """
        Aggregate multiple results of the supported type.

        Default implementation returns the first result if only one,
        otherwise raises NotImplementedError.

        Parameters
        ----------
        results : List[Any]
            List of results to aggregate

        Returns
        -------
        Any
            Aggregated result

        Raises
        ------
        NotImplementedError
            If aggregation is not supported and multiple results provided
        """
        if len(results) == 1:
            return results[0]
        raise NotImplementedError(
            f"Aggregation not implemented for {self.__class__.__name__}"
        )

    @property
    def supports_chunking(self) -> bool:
        """
        Whether this serializer supports chunking operations.

        Returns
        -------
        bool
            True if chunking is supported, False otherwise
        """
        return False

    def dumps(self, obj: Any) -> bytes:
        """
        Serialize an object to bytes.

        Convenience wrapper that returns just the bytes without metadata.

        Parameters
        ----------
        obj : Any
            The object to serialize

        Returns
        -------
        bytes
            The serialized bytes
        """
        data_bytes, _ = self.serialize(obj)
        return data_bytes

    def loads(self, data: bytes) -> Any:
        """
        Deserialize an object from bytes.

        Convenience wrapper for simple bytes-in/object-out interface.

        Parameters
        ----------
        data : bytes
            The serialized bytes

        Returns
        -------
        Any
            The deserialized object
        """
        return self.deserialize(data)
