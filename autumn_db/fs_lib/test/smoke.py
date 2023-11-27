import json
import random
import string
import unittest
import timeit

from fs_lib.data_access import FSOperations, FSCollectionService
from fs_lib.data_access.impl import FSOperationsMockImpl, FSOperationsImpl, FSCollectionMeta, FSServiceOperations

doc_count = 1000

fs_collection_meta = FSCollectionMeta('users')
fs_service_opers = FSServiceOperations(fs_collection_meta)

fs_opers = FSOperationsMockImpl(fs_collection_meta)
fs_opers_optimized = FSOperationsImpl(fs_collection_meta)

def generate_random_string(data_len: int = 8) -> str:
    return ''.join(random.choice(string.printable) for _ in range(data_len))
def generate_random_user_data() -> dict:
    return {
        'firstname': generate_random_string(),
        'lastname': generate_random_string(),
        'age': random.randint(0, 100),
        'address': generate_random_string(12),
        'city': generate_random_string(5),
        'country': generate_random_string(6),
        'job': generate_random_string(6)
    }

class TestCRUD(unittest.TestCase):

    def setUp(self):
        fs_service_opers.create_collection()

    def test_crud_success(self):

        self._filenames = list()
        random_data = generate_random_user_data()
        for i in range(doc_count):
            filename = f"test_data_{i}"
            self._filenames.append(filename)
            fs_opers_optimized.create(filename, json.dumps(random_data))

        def is_correct_data(fs_driver: FSOperations):
            for filename in self._filenames:
                if fs_driver.read(filename) == json.dumps(random_data):
                    return True
            return False

        res_optimized = is_correct_data(fs_opers_optimized)

        updated_data = generate_random_user_data()
        for i in range(doc_count):
            filename = f"test_data_{i}"
            self._filenames.append(filename)
            fs_opers_optimized.update(filename, json.dumps(updated_data))

        def is_correct_updated(fs_driver: FSOperations):
            for filename in self._filenames:
                if fs_driver.read(filename) == json.dumps(updated_data):
                    return True
            return False

        res_optimized = res_optimized and is_correct_updated(fs_opers_optimized)
        self.assertTrue(res_optimized)

    def test_metadata_success(self):
        pass

    def tearDown(self):
        fs_service_opers.remove_collection()