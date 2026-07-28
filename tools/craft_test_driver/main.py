# main.py
import argparse
import json
import sys
import time
import uuid
from pathlib import Path

from cluster import ClusterManager
from craft_disk import CraftDisk
from volumes import VolumeRegistry


def parse_args():
    p = argparse.ArgumentParser(description="CRAFT reference cluster test harness")
    p.add_argument("--config", required=True, type=Path,
                    help="server_config.json path (members: uuid, host, raft_port, tcp_port)")
    p.add_argument("--tcp_srv_binary", default=Path("./craft_reference_tcp_srv"), type=Path,
                    help="path to the craft_reference_tcp_srv executable")
    p.add_argument("--craft_disk_binary", type=Path,
                    help="path to the ublkpp_disk executable")
    p.add_argument("--startup-wait", type=float, default=1.0,
                    help="seconds to wait after spawning servers before assuming they are up")
    p.add_argument("--vol-id", type=str, default=None,
                    help="volume UUID to create (default: random, printed for reuse)")
    p.add_argument("--capacity", type=int, default=1 << 30, help="volume capacity in bytes")
    p.add_argument("--lba-size", type=int, default=4096, help="volume block size in bytes")
    p.add_argument("--cleanup", action="store_true",
                    help="kill any craft_reference_tcp_srv / ublkpp_disk processes left over, then exit")
    args = p.parse_args()
    args.tcp_srv_binary = args.tcp_srv_binary.resolve()
    if args.craft_disk_binary:
        args.craft_disk_binary = args.craft_disk_binary.resolve()
    return args


def load_members(config_path: Path) -> list[dict]:
    with open(config_path) as f:
        cfg = json.load(f)
    members = cfg["members"]
    if not members:
        raise ValueError(f"{config_path}: no members defined")
    return members


def cleanup_stray_processes():
    # Best-effort kill of leftover craft_reference_tcp_srv / ublkpp_disk processes by name
    import subprocess
    for name in ("craft_reference_tcp_srv", "ublkpp_disk"):
        result = subprocess.run(["pkill", "-9", "-f", name], capture_output=True)
        if result.returncode == 0:
            print(f"killed process(es) matching '{name}'")
        elif result.returncode == 1:
            print(f"no process matching '{name}' found")
        else:
            print(f"pkill for '{name}' failed: {result.stderr.decode().strip()}", file=sys.stderr)


def run(args, members):
    vol_id = uuid.UUID(args.vol_id) if args.vol_id else None
    cluster = ClusterManager(args.tcp_srv_binary, args.config, members)
    disk = None

    try:
        cluster.start_all()
        print(f"waiting {args.startup_wait}s for servers to come up...")
        time.sleep(args.startup_wait)

        dead = cluster.any_dead()
        if dead:
            raise RuntimeError(f"servers failed to start: {dead}")
        print("all server processes alive, proceeding to create_volume")

        registry = VolumeRegistry()
        vol = registry.create(members, vol_id=vol_id, capacity=args.capacity, lba_size=args.lba_size)
        print(f"volume created, waiting {args.startup_wait}s before attaching client...")
        time.sleep(args.startup_wait)

        if args.craft_disk_binary:
            disk = CraftDisk(args.craft_disk_binary, vol)
            disk.start()
            print(f"{len(members)} server(s) running, {vol}, disk at {disk.device_path}, "
                  f"press Ctrl+C to stop")
        else:
            print(f"{len(members)} server(s) running, {vol}, press Ctrl+C to stop")

        while True:
            time.sleep(1)
            dead = cluster.any_dead()
            if dead:
                raise RuntimeError(f"server(s) died unexpectedly: {dead}")
            if disk is not None and not disk.is_alive():
                raise RuntimeError("ublkpp_disk exited unexpectedly")

    except KeyboardInterrupt:
        print("Ctrl+C -- shutting down...")
        if disk is not None:
            disk.stop()
        cluster.shutdown()

    except Exception as e:
        print(f"error: {e}", file=sys.stderr)   
        print("leaving processes running for inspection. "
                "PIDs:", file=sys.stderr)
        for uuid_, proc in cluster.procs.items():
            print(f"  server {uuid_}: pid={proc.pid}", file=sys.stderr)
        if disk is not None and disk.proc is not None:
            print(f"  ublkpp_disk: pid={disk.proc.pid}", file=sys.stderr)
        print("re-run with --cleanup to kill them later.", file=sys.stderr)
        sys.exit(1)


def main():
    args = parse_args()

    if args.cleanup:
        cleanup_stray_processes()
        return

    members = load_members(args.config)
    run(args, members)


if __name__ == "__main__":
    main()