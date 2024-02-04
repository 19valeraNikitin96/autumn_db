import json
import socket
from enum import Enum

from autumn_db.autumn_db import DatabaseOperations


class CRUDOperation(Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3
    READ = 4


class CRUDReceiver:
    BUFFER_SIZE = 1

    def __init__(self, port: int, db_opers: DatabaseOperations):
        self._db_opers = db_opers

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._port = port
        self._socket.bind(
            ('0.0.0.0', port)
        )
        self._socket.listen()

    def processing(self):
        connection, client_address = self._socket.accept()

        received = bytearray()
        while True:
            part = connection.recv(CRUDReceiver.BUFFER_SIZE)
            if not part:
                break

            received.extend(part)

        oper = received[0]
        received = received[1::]
        if CRUDOperation.CREATE.value == oper:
            self._db_opers.on_create(received)

        if CRUDOperation.READ.value == oper:
            self._db_opers.on_read(received)

        if CRUDOperation.UPDATE.value == oper:
            self._db_opers.on_update(received)

        if CRUDOperation.DELETE.value == oper:
            self._db_opers.on_delete(received)
