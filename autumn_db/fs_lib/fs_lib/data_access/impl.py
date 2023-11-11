import datetime
import json
import os
import shutil
import mmap

from fs_lib.data_access import FSOperations, ServiceOperations, CollectionMeta, FSCollectionService


class FSCollectionMeta(CollectionMeta):

    def __init__(self, collection_name: str):
        super().__init__(collection_name)

        meta_holder_name = '_metadata'
        self._metadata_holder_path = os.path.join(collection_name, meta_holder_name)

        data_holder_name = '_data'
        self._data_holder_path = os.path.join(collection_name, data_holder_name)

    @property
    def path_to_metadata(self) -> str:
        return self._metadata_holder_path

    @property
    def path_to_data(self) -> str:
        return self._data_holder_path


class FSServiceOperations(ServiceOperations):

    def __init__(self, collection_meta: FSCollectionMeta):
        super().__init__(collection_meta)
        self._fs_collection_meta = collection_meta

    def create_collection(self):
        collection_name = self._collection_meta.collection_name
        if os.path.exists(collection_name):
            raise RuntimeError(f"Could not create collection {collection_name}. Collection already exists")

        os.makedirs(self._fs_collection_meta.path_to_metadata)
        os.makedirs(self._fs_collection_meta.path_to_data)

    def remove_collection(self):
        collection_name = self._collection_meta.collection_name
        shutil.rmtree(collection_name)


class FSOperationsImpl(FSOperations):
    UPDATED_AT_KEY = 'updated_at'
    IS_FROZEN_KEY = 'is_frozen'

    def __init__(self, fs_collection_meta: FSCollectionMeta):
        super().__init__(fs_collection_meta)
        self._fs_collection_meta: FSCollectionMeta = fs_collection_meta

    def _prepare_path(self, filename: str, is_metadata: bool = False):
        full_filename = f"{filename}{self._filename_ending}"
        holder = self._fs_collection_meta.path_to_metadata if is_metadata else self._fs_collection_meta.path_to_data
        path = os.path.join(holder, full_filename)

        return path

    def create(self, filename: str, data: str):
        data_path = self._prepare_path(filename)
        metadata_path = self._prepare_path(filename, True)

        for path in [data_path, metadata_path]:
            if os.path.exists(path):
                raise RuntimeError(f"File {path} already exists")

        with open(data_path, 'w') as f:
            f.write(data)
            f.close()
        with open(metadata_path, 'w') as f:
            metadata = {
                FSOperationsImpl.UPDATED_AT_KEY: datetime.datetime.utcnow().strftime(
                    FSCollectionService.UTC_FORMAT),
                FSOperationsImpl.IS_FROZEN_KEY: False
            }
            _metadata = json.dumps(metadata)
            f.write(_metadata)

    def update(self, filename: str, data: str):
        data_path = self._prepare_path(filename)
        metadata_path = self._prepare_path(filename, True)

        for path in [data_path, metadata_path]:
            if not os.path.exists(path):
                raise RuntimeError(f"Could not update {path}. File does not exist")

        with open(data_path, 'r+b') as f:
            file = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_WRITE)
            startofline = file.rfind(b'\n', 0, len(file) - 1) + 1
            file.resize(startofline + len(data))
            file[startofline:] = data.encode()
            file.flush()
            file.close()
            f.close()

        with open(metadata_path, 'r+') as f:
            _metadata = f.read()
            metadata = json.loads(_metadata)
            metadata[FSOperationsImpl.UPDATED_AT_KEY] = datetime.datetime.utcnow().strftime(
                FSCollectionService.UTC_FORMAT)
            _metadata = json.dumps(metadata)
            f.write(_metadata)

    def read(self, filename: str) -> str:
        path = self._prepare_path(filename)

        if not os.path.exists(path):
            raise RuntimeError(f"Could not read {path}. File does not exist")

        with open(path, 'r') as f:
            file = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
            data = file.read()

        return data.decode('UTF-8')

    def delete(self, filename: str):
        data_path = self._prepare_path(filename)
        metadata_path = self._prepare_path(filename, True)

        os.remove(data_path)
        os.remove(metadata_path)


class FSOperationsMockImpl(FSOperations):
    UPDATED_AT_KEY = 'updated_at'
    IS_FROZEN_KEY = 'is_frozen'

    def __init__(self, fs_collection_meta: FSCollectionMeta):
        super().__init__(fs_collection_meta)
        self._fs_collection_meta: FSCollectionMeta = fs_collection_meta

    def _prepare_path(self, filename: str, is_metadata: bool = False):
        full_filename = f"{filename}{self._filename_ending}"
        holder = self._fs_collection_meta.path_to_metadata if is_metadata else self._fs_collection_meta.path_to_data
        path = os.path.join(holder, full_filename)

        return path

    def create(self, filename: str, data: str):
        data_path = self._prepare_path(filename)
        metadata_path = self._prepare_path(filename, True)

        for path in [data_path, metadata_path]:
            if os.path.exists(path):
                raise RuntimeError(f"File {path} already exists")

        with open(data_path, 'w') as f:
            f.write(data)
        with open(metadata_path, 'w') as f:
            metadata = {
                FSOperationsMockImpl.UPDATED_AT_KEY: datetime.datetime.utcnow().strftime(FSCollectionService.UTC_FORMAT),
                FSOperationsMockImpl.IS_FROZEN_KEY: False
            }
            _metadata = json.dumps(metadata)
            f.write(_metadata)

    def update(self, filename: str, data: str):
        data_path = self._prepare_path(filename)
        metadata_path = self._prepare_path(filename, True)

        for path in [data_path, metadata_path]:
            if not os.path.exists(path):
                raise RuntimeError(f"Could not update {path}. File does not exist")

        with open(data_path, 'w') as f:
            f.write(data)
        with open(metadata_path, 'r+') as f:
            _metadata = f.read()
            metadata = json.loads(_metadata)
            metadata[FSOperationsMockImpl.UPDATED_AT_KEY] = datetime.datetime.utcnow().strftime(FSCollectionService.UTC_FORMAT)
            _metadata = json.dumps(metadata)
            f.write(_metadata)

    def read(self, filename: str) -> str:
        path = self._prepare_path(filename)

        if not os.path.exists(path):
            raise RuntimeError(f"Could not read {path}. File does not exist")

        with open(path, 'r') as f:
            data = f.read()

        return data

    def delete(self, filename: str):
        data_path = self._prepare_path(filename)
        metadata_path = self._prepare_path(filename, True)

        os.remove(data_path)
        os.remove(metadata_path)
