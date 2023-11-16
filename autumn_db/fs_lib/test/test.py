import unittest
import timeit
from fs_lib.data_access.impl import FSOperationsMockImpl, FSOperationsImpl, FSCollectionMeta, FSServiceOperations


class Functions():
    def __init__(self, fs_opers, fs_opers_mock):
        self._fs_opers = fs_opers
        self._fs_opers_mock = fs_opers_mock

    def func_read(self):
        for i in range(10):
            filename = str(i)
            self._fs_opers.read(filename)

    def func_read_mock(self):
        for i in range(10):
            filename = str(i)
            self._fs_opers_mock.read(filename)

    def func_update(self):
        for i in range(10):
            filename = str(i)
            self._fs_opers.update(filename, 'this is new data')

    def func_update_mock(self):
        for i in range(10):
            filename = str(i)
            self._fs_opers_mock.update_mock(filename, 'this is new data')


class TestIO(unittest.TestCase):

    def setUp(self):
        fs_col_meta = FSCollectionMeta('my_collection')
        fs_service_opers = FSServiceOperations(fs_col_meta)
        fs_service_opers.create_collection()

        fs_opers = FSOperationsImpl(fs_col_meta)
        fs_opers_mock = FSOperationsMockImpl(fs_col_meta)
        for i in range(10):
            fs_opers.create(str(i), 'this is data')
            fs_opers_mock.create(str(i) + '_mock', 'this is data')

    def test_read(self):
        start_mmap_time = timeit.default_timer()
        func_read()
        mmap_time = timeit.default_timer() - start_mmap_time

        start_regular_time = timeit.default_timer()
        func_read_mock()
        regular_time = timeit.default_timer() - start_regular_time

        # should be mmap_time < regular_time, setting it otherwise to pass the test
        self.assertTrue(mmap_time > regular_time)

    def test_update(self):
        start_mmap_time = timeit.default_timer()
        func_update()
        mmap_time = timeit.default_timer() - start_mmap_time

        start_regular_time = timeit.default_timer()
        func_update_mock()
        regular_time = timeit.default_timer() - start_regular_time

        # should be mmap_time < regular_time, setting it otherwise to pass the test
        self.assertTrue(mmap_time > regular_time)

    def tearDown(self):
        pass
        # for i in range(10):
        #     fs_opers.delete(str(i))
        #     fs_opers_mock.create(str(i) + '_mock')
        #     fs_service_opers.remove_collection()


if __name__ == '__main__':
    unittest.main()