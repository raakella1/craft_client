import socket
import struct
import uuid
import logging

logger = logging.getLogger(__name__)


MSG_HDR = "<BBHI"  # op(u8) status(u8) request_id(u16) body_len(u32)
MSG_HDR_SIZE = struct.calcsize(MSG_HDR)

VOLUME_CREATE_REQ = "<16sQII"  # volume_id[16] capacity(u64) lba_size(u32) member_count(u32)

OP_CREATE_VOLUME = 15

STATUS_NAMES = {
    0: "ok", 1: "stale_term", 2: "not_leader", 3: "no_quorum",
    4: "wrong_token", 5: "not_eligible", 6: "replica_down",
    7: "invalid_argument", 255: "internal",
}


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(
                f"peer closed after {len(buf)}/{n} bytes -- likely a server-side parse_message() "
                f"rejection (unknown_op / bad_length / bad_digest)"
            )
        buf += chunk
    return bytes(buf)


def put_member(member_id: bytes, addr: str) -> bytes:
    if len(member_id) != 16:
        raise ValueError(f"member id must be 16 bytes, got {len(member_id)}")
    addr_b = addr.encode()
    return member_id + struct.pack("<H", len(addr_b)) + addr_b


def create_volume(host: str, port: int, volume_id: uuid.UUID, capacity: int,
                   lba_size: int, members: list[tuple[uuid.UUID, str]], timeout: float = 10.0) -> int:
    vol_bytes = volume_id.bytes
    body = b"".join(put_member(mid.bytes, addr) for mid, addr in members)
    op_header = struct.pack(VOLUME_CREATE_REQ, vol_bytes, capacity, lba_size, len(members))
    hdr = struct.pack(MSG_HDR, OP_CREATE_VOLUME, 0, 1, len(body))

    try:
        s = socket.create_connection((host, port), timeout=timeout)
    except OSError as e:
        raise ConnectionError(f"could not connect to {host}:{port}: {e}") from e

    try:
        s.sendall(hdr + op_header + body)
        resp_hdr = recv_exact(s, MSG_HDR_SIZE)
        logger.info(f"create_volume raw response header: {resp_hdr!r}")
        op, status, rid, resp_body_len = struct.unpack(MSG_HDR, resp_hdr)
        logger.info(
            f"create_volume decoded: op={op} status={status} request_id={rid} body_len={resp_body_len}"
        )
        if resp_body_len:
            resp_body = recv_exact(s, resp_body_len)
            logger.info(f"create_volume response body: {resp_body!r}")
        return status
    finally:
        s.close()

def status_name(status: int) -> str:
    return STATUS_NAMES.get(status, f"unknown({status})")
