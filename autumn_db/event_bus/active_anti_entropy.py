import json
import logging
import socket
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from queue import Queue

from typing import List

from algorithms import to_bytearray_from_values
from algorithms.ph2 import PH2
from algorithms.spectral_bloom_filter import SpectralBloomFilter
from autumn_db import DocumentId
from autumn_db.autumn_db import DBCoreEngine
from autumn_db.data_storage.collection import CollectionOperations
from autumn_db.event_bus import Event, Subscriber, DocumentOrientedEvent
from db_driver import CollectionName, Document, DRIVER_COLLECTION_NAME_LENGTH_BYTES, DRIVER_BYTEORDER, \
    DRIVER_DOCUMENT_ID_LENGTH, CollectionOperation, DocumentOperation, send_message_to


def calculate_sbf(_bytearray: bytearray) -> SpectralBloomFilter:
    res = SpectralBloomFilter()
    res.add(_bytearray)

    return res


def calculate_ph2(_bytearray: bytearray):
    res = PH2()
    res.append(_bytearray)

    return res


@dataclass
class Endpoint:
    addr: str
    port: int


@dataclass
class NodeConfig:
    snapshot_receiver: Endpoint
    document_receiver: Endpoint

    def __post_init__(self):
        self.snapshot_receiver = Endpoint(**self.snapshot_receiver)
        self.document_receiver = Endpoint(**self.document_receiver)


@dataclass
class AAEConfig:
    current: NodeConfig
    neighbors: List[NodeConfig]

    def __post_init__(self):
        self.current = NodeConfig(**self.current)
        self.neighbors = [NodeConfig(**entry) for entry in self.neighbors]


class DocumentReceiver:
    BUFFER_SIZE = 1

    def __init__(self, port: int):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._port = port
        self._socket.bind(
            ('0.0.0.0', port)
        )
        self._socket.settimeout(0.2)
        self._socket.listen()

    def get_document_and_metadata(self) -> bytearray:
        try:
            connection, client_address = self._socket.accept()
        except socket.timeout:
            return None

        data = bytearray()
        while True:
            part = connection.recv(DocumentReceiver.BUFFER_SIZE)
            if not part:
                break

            data.extend(part)

        return data


class AAEOperationType(Enum):
    TERMINATE_SESSION: int = 0
    SENDING_SNAPSHOT: int = 1
    SENDING_TIMESTAMP: int = 2

    @staticmethod
    def get_by_value(value: int):
        for t in AAEOperationType:
            if t.value == value:
                return t

        raise Exception(f"Could not map to the type value {value}")


class AAESnapshot:

    def get(self) -> bytearray: ...


class AAECommunication(AAESnapshot):

    def __init__(self, _type: AAEOperationType):
        self._type = _type

    def get_opcode(self) -> bytes:
        res = self._type.value.to_bytes(1, byteorder=DRIVER_BYTEORDER)
        return res

    def get(self) -> bytearray: ...


class Snapshot:

    def __init__(self, sbf: SpectralBloomFilter, hash: PH2):
        b_sbf = sbf.get()
        b_hash = hash.hashing()

        self._bytearray = bytearray()
        parts = [
            b_sbf,
            b_hash,
        ]
        for part in parts:
            self._bytearray.extend(part)

    def get(self) -> bytearray:
        return self._bytearray

    def __str__(self):
        return str(self._bytearray)

    def __repr__(self):
        return self.__str__()


class AAECheckSnapshot(AAECommunication):

    def __init__(self, collection_name: str, doc_id: str, snapshot: Snapshot):
        super().__init__(AAEOperationType.SENDING_SNAPSHOT)
        b_collection_name = collection_name.encode('utf-8')
        collection_name_len = len(b_collection_name)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                   DRIVER_BYTEORDER, signed=False)
        b_doc_id = doc_id.encode()

        self._bytearray = bytearray()
        parts = [
            self.get_opcode(),
            collection_name_len_encoded,
            b_collection_name,
            b_doc_id,
            snapshot.get(),
        ]
        for part in parts:
            self._bytearray.extend(part)

    def get(self) -> bytearray:
        return self._bytearray


class AAERequestSnapshot(AAECommunication):

    def __init__(self, collection_name: str, doc_id: str):
        super().__init__(AAEOperationType.SENDING_TIMESTAMP)
        b_collection_name = collection_name.encode('utf-8')
        collection_name_len = len(b_collection_name)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                   DRIVER_BYTEORDER, signed=False)

        b_doc_id = doc_id.encode()

        self._bytearray = bytearray()
        parts = [
            self.get_opcode(),
            collection_name_len_encoded,
            b_collection_name,
            b_doc_id
        ]
        for part in parts:
            self._bytearray.extend(part)

    def get(self) -> bytearray:
        return self._bytearray


class AAEAnswererWorker:
    BUFFER_SIZE = 46
    SENDING_TIMESTAMP_PAYLOAD_PART = bytes([AAEOperationType.SENDING_TIMESTAMP.value])
    TERMINATION_PAYLOAD = bytes([AAEOperationType.TERMINATE_SESSION.value])

    def __init__(self, addr: str, port: int, db_core: DBCoreEngine, receivers: List[NodeConfig]):
        super().__init__()
        self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self._socket.settimeout(0.2)
        self._socket.bind(
            (
                addr, port
            )
        )
        self._listening_port = port

        self._db_core = db_core
        self._receivers = receivers

    def processing(self):
        try:
            payload, addr_port = self._socket.recvfrom(AAEAnswererWorker.BUFFER_SIZE)
        except socket.timeout:
            return None

        oper_code = payload[0]
        payload = payload[1::1]
        operation_type = AAEOperationType.get_by_value(oper_code)

        if operation_type == AAEOperationType.SENDING_SNAPSHOT:
            collection_name_length_bytes = payload[:DRIVER_COLLECTION_NAME_LENGTH_BYTES:1]
            payload = payload[DRIVER_COLLECTION_NAME_LENGTH_BYTES::]

            collection_name_length = int.from_bytes(collection_name_length_bytes, DRIVER_BYTEORDER, signed=False)
            collection_name_bytes = payload[:collection_name_length:1]
            collection_name_str = collection_name_bytes.decode('utf-8')

            payload = payload[collection_name_length::1]

            doc_id = payload[:DRIVER_DOCUMENT_ID_LENGTH:1]
            doc_id = doc_id.decode('utf-8')
            payload = payload[DRIVER_DOCUMENT_ID_LENGTH::]

            snapshot = payload[::]

            collection: CollectionOperations = self._db_core.collections[collection_name_str]
            operator = collection.get_document_operator(doc_id)

            data = operator.read()
            _json = json.loads(data)

            _bytearray = to_bytearray_from_values(_json)

            sbf = calculate_sbf(_bytearray)
            ph2 = calculate_ph2(_bytearray)

            local_snapshot = Snapshot(sbf, ph2)

            if bytes(local_snapshot.get()) == snapshot:
                print(f"Sent to {addr_port} TERMINATION")
                self._socket.sendto(AAEAnswererWorker.TERMINATION_PAYLOAD, addr_port)
                return None

            metadata = collection.get_metadata_operator(doc_id)
            local_timestamp = metadata.get_updated_at()

            s_timestamp = datetime.strftime(local_timestamp, DocumentId.UTC_FORMAT)
            b_timestamp = s_timestamp.encode('utf-8')

            _bytearray = bytearray()
            parts = [
                AAEAnswererWorker.SENDING_TIMESTAMP_PAYLOAD_PART,
                b_timestamp,
            ]
            for part in parts:
                _bytearray.extend(part)

            print(f"Sent to {addr_port} SENDING_TIMESTAMP")
            self._socket.sendto(_bytearray, addr_port)


class ActiveAntiEntropy(Subscriber):

    def __init__(self, config: AAEConfig, db_core: DBCoreEngine):
        self._conf = config
        self._db_core = db_core

        self._doc_receiver = DocumentReceiver(self._conf.current.document_receiver.port)
        self._snapshot_receiver = AAEAnswererWorker(
            self._conf.current.snapshot_receiver.addr, self._conf.current.snapshot_receiver.port,
            self._db_core, self._conf.neighbors
        )

        self._document_event_queue = Queue()
        self._collection_event_queue = Queue()

    def callback(self, event: Event):
        doc_opers = [oper.value for oper in list(CollectionOperation)]

        if event.event_code in doc_opers:
            self._document_event_queue.put(event)
            return

        collection_opers = [oper.value for oper in list(DocumentOperation)]
        if event.event_code in collection_opers:
            self._document_event_queue.put(event)
            return

    def processing(self):
        while True:

            doc_and_metadata = self._doc_receiver.get_document_and_metadata()
            if doc_and_metadata is not None:
                collection, doc_id, doc, updated_at = self._parse_document_and_metadata(doc_and_metadata)
                self._write_doc(collection, doc_id, doc, updated_at)
                ev = DocumentOrientedEvent(collection, DocumentOperation.CREATE_DOC, doc_id)
                self._on_create_event(ev)

            if self._document_event_queue.qsize() > 0:
                ev: DocumentOrientedEvent = self._document_event_queue.get()

                if ev.event_code == DocumentOperation.UPDATE_DOC.value:
                    self._on_update_event(ev)

                if ev.event_code == DocumentOperation.CREATE_DOC.value:
                    self._on_create_event(ev)

            self._snapshot_receiver.processing()

    def _on_create_event(self, ev: DocumentOrientedEvent):
        db_collection: CollectionOperations = self._db_core.collections[ev.collection.name]
        filename = str(ev.document_id)
        metadata_oper = db_collection.get_metadata_operator(filename)
        data_oper = db_collection.get_document_operator(filename)

        timestamp = metadata_oper.get_updated_at()

        bytes_to_send = bytearray()
        collection_name_encoded = ev.collection.name.encode('utf-8')
        collection_name_len = len(collection_name_encoded)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                   DRIVER_BYTEORDER, signed=False)
        doc_id_encoded = str(ev.document_id).encode('utf-8')
        updated_at_encoded = datetime.strftime(timestamp, DocumentId.UTC_FORMAT).encode('utf-8')

        doc = data_oper.read()

        bytes_to_send.extend(collection_name_len_encoded)
        bytes_to_send.extend(collection_name_encoded)
        bytes_to_send.extend(doc_id_encoded)
        bytes_to_send.extend(updated_at_encoded)
        bytes_to_send.extend(doc.encode('utf-8'))

        for neigh in self._conf.neighbors:
            addr_port = (neigh.document_receiver.addr, neigh.document_receiver.port)
            send_message_to(
                addr_port,
                bytes_to_send
            )

    def _on_update_event(self, ev: DocumentOrientedEvent):
        doc_id = str(ev.document_id)

        collection: CollectionOperations = self._db_core.collections[ev.collection.name]

        operator = collection.get_document_operator(doc_id)

        data = operator.read()
        _json = json.loads(data)

        _bytearray = to_bytearray_from_values(_json)

        sbf = calculate_sbf(_bytearray)
        ph2 = calculate_ph2(_bytearray)

        snapshot = Snapshot(sbf, ph2)
        check_snapshot = AAECheckSnapshot(ev.collection.name, doc_id, snapshot)
        b_check_snapshot = check_snapshot.get()

        for neigh in self._conf.neighbors:
            sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

            sock.settimeout(3)

            receiver_addr_port = (neigh.snapshot_receiver.addr, neigh.snapshot_receiver.port)
            print(f"Sent to {receiver_addr_port} AAECheckSnapshot")
            sock.sendto(b_check_snapshot,
                        receiver_addr_port
                        )
            print(f'Socket to send AAECheckSnapshot {sock.getsockname()}')

            try:
                payload, server_addr_port = sock.recvfrom(48)
            except socket.timeout:
                print(f'Continue {sock.getsockname()}')
                continue
            resp_type = AAEOperationType.get_by_value(payload[0])

            if resp_type == AAEOperationType.TERMINATE_SESSION:
                continue

            if resp_type == AAEOperationType.SENDING_TIMESTAMP:
                b_timestamp = payload[1:]
                s_timestamp = b_timestamp.decode()
                timestamp = datetime.strptime(s_timestamp, DocumentId.UTC_FORMAT)

                metadata = collection.get_metadata_operator(doc_id)
                local_timestamp = metadata.get_updated_at()

                if local_timestamp > timestamp:
                    recv_doc_addr_port = (neigh.document_receiver.addr, neigh.document_receiver.port)

                    bytes_to_send = bytearray()
                    collection_name_encoded = ev.collection.name.encode('utf-8')
                    collection_name_len = len(collection_name_encoded)
                    collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                               DRIVER_BYTEORDER, signed=False)
                    doc_id_encoded = doc_id.encode('utf-8')
                    updated_at_encoded = datetime.strftime(local_timestamp, DocumentId.UTC_FORMAT).encode('utf-8')

                    bytes_to_send.extend(collection_name_len_encoded)
                    bytes_to_send.extend(collection_name_encoded)
                    bytes_to_send.extend(doc_id_encoded)
                    bytes_to_send.extend(updated_at_encoded)
                    bytes_to_send.extend(data.encode('utf-8'))

                    send_message_to(
                        recv_doc_addr_port,
                        bytes_to_send
                    )

    @staticmethod
    def _parse_document_and_metadata(src: bytearray):
        # FORMAT
        # |COLLECTION_NAME_LENGTH|COLLECTION_NAME|  DOC_ID  |UPDATED_AT| DOCUMENT |
        #           1byte             1-255bytes   26bytes      26bytes    Xbytes
        collection_name_length_bytes = src[:DRIVER_COLLECTION_NAME_LENGTH_BYTES:1]
        src = src[DRIVER_COLLECTION_NAME_LENGTH_BYTES::]

        collection_name_length = int.from_bytes(collection_name_length_bytes, DRIVER_BYTEORDER, signed=False)
        collection_name_bytes = src[:collection_name_length:1]
        collection_name_str = collection_name_bytes.decode('utf-8')
        collection_name = CollectionName(collection_name_str)

        src = src[collection_name_length::]

        doc_id_bytes = src[:DRIVER_DOCUMENT_ID_LENGTH:]
        doc_id = doc_id_bytes.decode('utf-8')
        print(doc_id)
        doc_id = DocumentId(doc_id)

        src = src[DRIVER_DOCUMENT_ID_LENGTH::]

        UPDATED_AT_LENGTH = 26
        updated_at_bytes = src[:UPDATED_AT_LENGTH:]
        updated_at_str = updated_at_bytes.decode('utf-8')
        updated_at = datetime.strptime(updated_at_str, DocumentId.UTC_FORMAT)

        src = src[UPDATED_AT_LENGTH::]

        doc_str = src.decode('utf-8')
        doc = Document(doc_str)

        return collection_name, doc_id, doc, updated_at

    def _write_doc(self, collection: CollectionName, doc_id: DocumentId, doc: Document, updated_at: datetime):
        db_collection: CollectionOperations = self._db_core.collections[collection.name]
        filename = str(doc_id)
        metadata_oper = db_collection.get_metadata_operator(filename)

        if not db_collection.document_exists(filename):
            db_collection.create_document(filename, doc.document)
            metadata_oper.set_updated_at(updated_at)
            return

        metadata_oper.set_is_frozen(True)

        local_updated_at = metadata_oper.get_updated_at()
        if local_updated_at >= updated_at:
            return

        document_oper = db_collection.get_document_operator(filename)
        document_oper.update(doc.document)

        metadata_oper.set_updated_at(updated_at)
        metadata_oper.set_is_frozen(False)
