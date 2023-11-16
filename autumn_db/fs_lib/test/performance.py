import json
import random
import string
import unittest
import timeit

from fs_lib.data_access import FSOperations
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


class TestIO(unittest.TestCase):

    def setUp(self):
        fs_service_opers.create_collection()

        self._filenames = list()
        random_data = generate_random_user_data()
        for i in range(doc_count):
            filename = f"test_data_{i}"
            self._filenames.append(filename)
            fs_opers_optimized.create(filename, json.dumps(random_data))

    def test_read_success(self):
        def measurement(fs_driver: FSOperations) -> float:
            time_before = timeit.default_timer()
            for filename in self._filenames:
                fs_driver.read(filename)
            time_after = timeit.default_timer()

            return time_after - time_before

        fs_opers_res = measurement(fs_opers)
        fs_opers_optimized_res = measurement(fs_opers_optimized)

        res = fs_opers_optimized_res < fs_opers_res
        if not res:
            print(f"Optimized version is worst comparing with builtin I/O for the reading documents")

        print(f"Builtin I/O: {fs_opers_res} s")
        print(f"Optimized version: {fs_opers_optimized_res} s")
        self.assertTrue(res)

    def test_update_success(self):
        new_data_json = generate_random_user_data()
        new_data = json.dumps(new_data_json)

        def measurement(fs_driver: FSOperations) -> float:
            time_before = timeit.default_timer()
            for filename in self._filenames:
                fs_driver.update(filename, new_data)
            time_after = timeit.default_timer()

            return time_after - time_before

        fs_opers_optimized_res = measurement(fs_opers_optimized)
        fs_opers_res = measurement(fs_opers)

        res = fs_opers_optimized_res < fs_opers_res
        if not res:
            print(f"Optimized version is worst comparing with builtin I/O for the updating documents")

        print(f"Builtin I/O: {fs_opers_res} s")
        print(f"Optimized version: {fs_opers_optimized_res} s")
        self.assertTrue(res)

    def tearDown(self):
        fs_service_opers.remove_collection()
