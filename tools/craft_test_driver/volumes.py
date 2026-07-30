# volumes.py
import uuid
import logging

from craft_wire import create_volume, status_name

logger = logging.getLogger(__name__)


class Volume:
    def __init__(self, vol_id: uuid.UUID, members: list[dict], capacity: int, lba_size: int):
        self.vol_id = vol_id
        self.members = members  # subset of cluster members hosting this volume
        self.capacity = capacity
        self.lba_size = lba_size

    def __repr__(self):
        return f"Volume({self.vol_id}, members={len(self.members)}, capacity={self.capacity})"


class VolumeRegistry:
    """Test-harness-side bookkeeping of volumes created on the cluster -- not server state, just
    what this test run knows it asked for, so later steps (client attach, fault injection) can
    look volumes up instead of threading vol_id/members through every function call."""

    def __init__(self):
        self._volumes: dict[uuid.UUID, Volume] = {}

    def create(self, members: list[dict], vol_id: uuid.UUID = None,
               capacity: int = 1 << 30, lba_size: int = 4096) -> Volume:
        vol_id = vol_id or uuid.uuid4()
        leader = members[0]  # TODO: revisit once leader isn't assumed to be members[0]
        wire_members = [(uuid.UUID(m["uuid"]), f"{m['host']}:{m['tcp_port']}") for m in members]

        status = create_volume(leader["host"], leader["tcp_port"], vol_id, capacity, lba_size, wire_members)
        name = status_name(status)
        logger.info(f"create_volume({vol_id}) -> status={status} ({name})")
        if status != 0:
            raise RuntimeError(f"create_volume failed: {name}")

        vol = Volume(vol_id, members, capacity, lba_size)
        self._volumes[vol_id] = vol
        return vol

    def get(self, vol_id: uuid.UUID) -> Volume:
        return self._volumes[vol_id]

    def all(self) -> list[Volume]:
        return list(self._volumes.values())
