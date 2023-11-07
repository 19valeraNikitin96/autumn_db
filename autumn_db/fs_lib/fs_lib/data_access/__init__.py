

class CollectionMeta:

    def __init__(self, collection_name: str):
        self._collection_name = collection_name

    @property
    def collection_name(self) -> str:
        return self._collection_name


class ServiceOperations:

    def __init__(self, collection_meta: CollectionMeta):
        self._collection_meta = collection_meta

    def create_collection(self): ...

    def remove_collection(self): ...


class FSCollectionService:
    UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, collection_meta: CollectionMeta):
        self._collection_meta = collection_meta

    def create(self, filename: str, data: str): ...

    def update(self, filename: str, data: str): ...

    def read(self, filename: str) -> str: ...

    def delete(self, filename: str): ...


class FSOperations(FSCollectionService):

    def __init__(self, collection_meta: CollectionMeta):
        super().__init__(collection_meta)
        self._extension = 'json'
        self._filename_ending = f".{self._extension}"
