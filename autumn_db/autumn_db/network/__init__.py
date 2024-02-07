import socket
import threading

from autumn_db.autumn_db import DBCoreEngine, DBOperationEngine, CreateOperation
from db_driver import DRIVER_COLLECTION_NAME_LENGTH_BYTES as COLLECTION_NAME_LENGTH_BYTES, DRIVER_OPERATION_LENGTH
from db_driver import DRIVER_BYTEORDER as BYTEORDER
from db_driver import DBNetworkOperation as DBOperation


# MESSAGE format
# |OpCode|Collection name length|Collection name|Data   |
#  1byte        1byte               1-255bytes   Xbytes
class ClientEndpoint:
    BUFFER_SIZE = 1

    def __init__(self, port: int, db_core: DBCoreEngine):
        self._db_core = db_core

        self._db_opers = DBOperationEngine(db_core)
        th = threading.Thread(target=self._db_opers.processing, args=())
        th.start()

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._port = port
        self._socket.bind(
            ('0.0.0.0', port)
        )
        self._socket.listen()

    def processing(self):
        while True:
            connection, client_address = self._socket.accept()

            received = bytearray()
            while True:
                part = connection.recv(ClientEndpoint.BUFFER_SIZE)
                if not part:
                    break

                received.extend(part)

            oper = received[0]
            received = received[DRIVER_OPERATION_LENGTH::]
            if DBOperation.CREATE_DOC.value == oper:
                collection_name_length_bytes = received[:COLLECTION_NAME_LENGTH_BYTES:1]
                received = received[COLLECTION_NAME_LENGTH_BYTES::]

                collection_name_length = int.from_bytes(collection_name_length_bytes, BYTEORDER, signed=False)
                collection_name_bytes = received[:collection_name_length:1]
                collection_name = collection_name_bytes.decode('utf-8')

                received = received[collection_name_length::]
                doc_str = received.decode('utf-8')
                oper = CreateOperation(collection_name, doc_str)
                self._db_opers.add_operation(oper)

                continue

            if DBOperation.READ_DOC.value == oper:
                pass

            if DBOperation.UPDATE_DOC.value == oper:
                pass

            if DBOperation.DELETE_DOC.value == oper:
                pass
