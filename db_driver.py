import json
import math
import socket
from enum import Enum

from autumn_db import DocumentId

DRIVER_OPERATION_LENGTH = 1
DRIVER_COLLECTION_NAME_LENGTH_BYTES = 1
DRIVER_COLLECTION_NAME_LENGTH_BYTES_MAX = 255
DRIVER_BYTEORDER = 'big'


class DBNetworkOperation(Enum):
    CREATE_DOC = 1
    UPDATE_DOC = 2
    DELETE_DOC = 3
    READ_DOC = 4

    CREATE_COLLECTION = 11
    DELETE_COLLECTION = 12


class CollectionName:
    COLLECTION_NAME_LENGTH = math.pow(2, DRIVER_COLLECTION_NAME_LENGTH_BYTES)

    def __init__(self, name: str):
        self._validate(name)
        self._name = name

    def _validate(self, name: str):
        name_len = len(name.encode('utf-8'))
        if name_len > DRIVER_COLLECTION_NAME_LENGTH_BYTES_MAX:
            raise Exception(f"The collection name can contain {CollectionName.COLLECTION_NAME_LENGTH} chars as max")

    @property
    def name(self) -> str:
        return self._name


class Document:

    def __init__(self, doc: str):
        self._validate(doc)
        self._doc = doc

    def _validate(self, doc: str):
        json.loads(doc)

    @property
    def document(self) -> str:
        return self._doc


def send_message_to(addr_port: tuple, message: bytes, expect_response: bool = False) -> bytearray:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(addr_port)
    s.sendall(message)

    resp = None
    if expect_response:
        resp = bytearray()
        while True:
            part = s.recv(1)
            if not part or part == b'\x00':
                break

            resp.extend(part)

    s.close()
    return resp


class DBDriver:

    def __init__(self, addr: str, port: int = 50000):
        self._addr = addr
        self._port = port

    def create_collection(self, name: CollectionName):
        pass

    def create_document(self, collection: CollectionName, doc: Document):

        oper_bytes = DBNetworkOperation.CREATE_DOC.value.to_bytes(DRIVER_OPERATION_LENGTH, DRIVER_BYTEORDER, signed=False)

        collection_name_bytes = collection.name.encode('utf-8')

        collection_name_len = len(collection_name_bytes)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES, DRIVER_BYTEORDER, signed=False)

        doc_bytes = doc.document.encode('utf-8')

        _bytes = list()
        for b in oper_bytes:
            _bytes.append(b)

        for b in collection_name_len_encoded:
            _bytes.append(b)

        for b in collection_name_bytes:
            _bytes.append(b)

        for b in doc_bytes:
            _bytes.append(b)

        _bytes = bytearray(_bytes)
        _bytes.extend(b'\x00')

        doc_id_bytes = send_message_to(
            (self._addr, self._port), _bytes, expect_response=True
        )

        doc_id = doc_id_bytes.decode('utf-8')
        return doc_id

    def read_document(self, collection: CollectionName, doc_id: DocumentId):
        oper_bytes = DBNetworkOperation.READ_DOC.value.to_bytes(DRIVER_OPERATION_LENGTH, DRIVER_BYTEORDER,
                                                                  signed=False)

        collection_name_bytes = collection.name.encode('utf-8')

        collection_name_len = len(collection_name_bytes)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                   DRIVER_BYTEORDER, signed=False)

        doc_id_bytes = str(doc_id).encode('utf-8')

        _bytes = bytearray()

        _bytes.extend(oper_bytes)
        _bytes.extend(collection_name_len_encoded)
        _bytes.extend(collection_name_bytes)
        _bytes.extend(doc_id_bytes)
        _bytes.extend(b'\x00')

        doc_bytes = send_message_to(
            (self._addr, self._port), _bytes, expect_response=True
        )

        doc = doc_bytes.decode('utf-8')
        res = Document(doc)
        return res
