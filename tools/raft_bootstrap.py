import socket
import struct
import sys
import uuid

# msg_hdr: op(u8) status(u8) request_id(u16) body_len(u32) -- 8 bytes, little-endian
MSG_HDR = "<BBHI"
MSG_HDR_SIZE = struct.calcsize(MSG_HDR)

# volume_create_req: volume_id[16] capacity(u64) lba_size(u32) member_count(u32) -- 32 bytes
VOLUME_CREATE_REQ = "<16sQII"

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
                f"rejection (unknown_op / bad_length / bad_digest);"
            )
        buf += chunk
    return bytes(buf)


def put_member(member_id: bytes, addr: str) -> bytes:
    if len(member_id) != 16:
        raise ValueError(f"member id must be 16 bytes, got {len(member_id)}")
    addr_b = addr.encode()
    return member_id + struct.pack("<H", len(addr_b)) + addr_b


def create_volume(host: str, port: int, volume_id: bytes, capacity: int,
                   lba_size: int, members: list[tuple[bytes, str]], timeout: float = 10.0) -> int:
    if len(volume_id) != 16:
        raise ValueError(f"volume_id must be 16 bytes, got {len(volume_id)}")

    body = b"".join(put_member(mid, addr) for mid, addr in members)
    op_header = struct.pack(VOLUME_CREATE_REQ, volume_id, capacity, lba_size, len(members))
    body_len = len(body)
    hdr = struct.pack(MSG_HDR, OP_CREATE_VOLUME, 0, 1, body_len)

    try:
        s = socket.create_connection((host, port), timeout=timeout)
    except OSError as e:
        raise ConnectionError(f"could not connect to {host}:{port}: {e}") from e

    try:
        s.sendall(hdr + op_header + body)
        resp_hdr = recv_exact(s, MSG_HDR_SIZE)
        op, status, request_id, resp_body_len = struct.unpack(MSG_HDR, resp_hdr)
        if resp_body_len:
            recv_exact(s, resp_body_len)
        return status
    finally:
        s.close()


if __name__ == "__main__":
    volume_id = (1).to_bytes(16, "little")

    #member_ids = [uuid.uuid4() for _ in range(3)]
    # Fixed for debugging -- swap back to uuid.uuid4() once the flow is finalized.
    member_ids = [
        uuid.UUID("11111111-1111-1111-1111-111111111111"),
        uuid.UUID("22222222-2222-2222-2222-222222222222"),
        uuid.UUID("33333333-3333-3333-3333-333333333333"),
    ]
    addrs = ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"]
    members = [(mid.bytes, addr) for mid, addr in zip(member_ids, addrs)]

    for mid, addr in zip(member_ids, addrs):
        print(f"member {mid} @ {addr}")

    try:
        status = create_volume("127.0.0.1", 7001, volume_id, capacity=1 << 30,
                               lba_size=4096, members=members)
    except (ConnectionError, ValueError, socket.timeout) as e:
        print(f"create_volume failed: {e}", file=sys.stderr)
        sys.exit(1)

    name = STATUS_NAMES.get(status, f"unknown({status})")
    print(f"status: {status} ({name})")
    sys.exit(0 if status == 0 else 1)