import datetime
import json
import os
import shutil
print('Ok!!!!!')
from autumn_db.autumn_db import DocumentId
print('Ok!!!!')
from autumn_db.data_storage.collection import FileOperations, MetadataOperations, CollectionOperations, file_access


class MetadataOperationsImpl(MetadataOperations):
    UPDATED_AT_KEY = 'updated_at'
    IS_FROZEN_KEY = 'is_frozen'

    def set_updated_at(self, _datetime: datetime.datetime):
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)

        metadata[MetadataOperationsImpl.UPDATED_AT_KEY] = _datetime.strftime(DocumentId.UTC_FORMAT)
        content = json.dumps(metadata)

        self._file_access.update(self._pathname, content)

    def get_updated_at(self) -> datetime.datetime:
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)
        updated_at_str = metadata[MetadataOperationsImpl.UPDATED_AT_KEY]

        res = datetime.datetime.strptime(updated_at_str, DocumentId.UTC_FORMAT)
        return res

    def is_frozen(self) -> bool:
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)

        res = metadata[MetadataOperationsImpl.IS_FROZEN_KEY]
        return res

    def set_is_frozen(self, is_frozen: bool):
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)

        metadata[MetadataOperationsImpl.IS_FROZEN_KEY] = is_frozen
        content = json.dumps(metadata)

        self._file_access.update(self._pathname, content)


class FileOperationsImpl(FileOperations):

    def create(self, file: str):
        self._file_access.create(self._pathname, file)

    def update(self, file: str):
        self._file_access.update(self._pathname, file)

    def read(self) -> str:
        res = self._file_access.read(self._pathname)

        return res


class CollectionOperationsImpl(CollectionOperations):

    def create(self):
        path_to_data = os.path.join(self._full_path_to_collection, 'data')
        path_to_metadata = os.path.join(self._full_path_to_collection, 'metadata')
        os.makedirs(path_to_data)
        os.makedirs(path_to_metadata)

    def delete(self):
        shutil.rmtree(self._full_path_to_collection)

    def create_document(self, filename: str, data: str):
        data_pathname = os.path.join(self._full_path_to_collection, 'data', filename)
        metadata_pathname = os.path.join(self._full_path_to_collection, 'metadata', filename)

        file_access.create(data_pathname, data)

        metadata_content = {
            MetadataOperationsImpl.UPDATED_AT_KEY: datetime.datetime.utcnow().strftime(
                    DocumentId.UTC_FORMAT),
            MetadataOperationsImpl.IS_FROZEN_KEY: False
        }
        metadata_content_str = json.dumps(metadata_content)
        file_access.create(metadata_pathname, metadata_content_str)

    def delete_document(self, filename: str):
        data_pathname = os.path.join(self._full_path_to_collection, 'data', filename)
        metadata_pathname = os.path.join(self._full_path_to_collection, 'metadata', filename)

        file_access.delete(data_pathname)
        file_access.delete(metadata_pathname)

    def get_document_operator(self, filename: str) -> FileOperations:
        pathname = os.path.join(self._full_path_to_collection, 'data', filename)

        res = FileOperationsImpl(pathname)
        return res

    def get_metadata_operator(self, filename: str) -> MetadataOperations:
        pathname = os.path.join(self._full_path_to_collection, 'metadata', filename)

        res = MetadataOperationsImpl(pathname)
        return res
