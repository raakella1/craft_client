"""Process-global registry of everything the harness has created: cluster, volumes, disks.
Tests import this module directly instead of receiving these as parameters."""

from cluster import ClusterManager
from volumes import VolumeRegistry

cluster: ClusterManager | None = None
volumes: VolumeRegistry = VolumeRegistry()
disks: dict = {}  # vol_id (str) -> CraftDisk


def set_cluster(c: ClusterManager) -> None:
    global cluster
    cluster = c


def add_disk(vol_id: str, disk) -> None:
    disks[vol_id] = disk


def get_disk(vol_id: str):
    return disks[vol_id]


def all_disks() -> list:
    return list(disks.values())