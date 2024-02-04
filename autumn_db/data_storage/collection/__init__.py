import datetime

from autumn_db.data_storage.data_access.impl import FilesystemAccess


file_access = FilesystemAccess()


class Operations(object):

    def __init__(self, pathname: str):
        self._pathname = pathname
        self._file_access = file_access


class FileOperations(Operations):

    def create(self, file: str): ...

    def update(self, file: str): ...

    def read(self) -> str: ...


class MetadataOperations(Operations):

    def set_updated_at(self, _datetime: datetime.datetime): ...

    def get_updated_at(self) -> datetime.datetime: ...

    def is_frozen(self) -> bool: ...

    def set_is_frozen(self, is_frozen: bool): ...


class CollectionOperations(object):

    def __init__(self, name: str, data_holder_path: str = None):
        import os
        if data_holder_path is None:
            data_holder_path = os.getcwd()

        self._data_holder_path = data_holder_path
        self._name = name
        self._full_path_to_collection = os.path.join(self._data_holder_path, self._name)

    @property
    def name(self) -> str:
        return self._name

    def create_document(self, filename: str, data: str): ...

    def delete_document(self, filename: str): ...

    def create(self): ...

    def delete(self): ...

    def get_document_operator(self, filename: str) -> FileOperations: ...

    def get_metadata_operator(self, filename: str) -> MetadataOperations: ...
