
def mock_example():
    from fs_lib.data_access.impl import FSOperationsMockImpl, FSCollectionMeta, FSServiceOperations

    fs_col_meta = FSCollectionMeta('my_collection')

    fs_service_opers = FSServiceOperations(fs_col_meta)
    fs_service_opers.create_collection()

    fs_opers = FSOperationsMockImpl(fs_col_meta)
    fs_opers.create('123', '456')
    fs_opers.update('123', '789')

    content = fs_opers.read('123')
    print(content)

    fs_opers.delete('123')

    fs_service_opers.remove_collection()


def example():
    from fs_lib.data_access.impl import FSOperationsImpl, FSCollectionMeta, FSServiceOperations

    fs_col_meta = FSCollectionMeta('my_collection')

    fs_service_opers = FSServiceOperations(fs_col_meta)
    fs_service_opers.create_collection()

    fs_opers = FSOperationsImpl(fs_col_meta)
    fs_opers.create('123', '456')
    fs_opers.update('123', '789')

    content = fs_opers.read('123')
    print(content)

    fs_opers.delete('123')

    fs_service_opers.remove_collection()


mock_example()
# example()
