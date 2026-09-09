import argparse
import json
import sys
import time
import uuid
from pathlib import Path
import logging

from cluster import ClusterManager
from volumes import VolumeRegistry
from craft_disk import CraftDisk
import registry
from test_registry import run_test, TestNotFoundError, list_tests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="CRAFT reference cluster test harness")
    p.add_argument(
        "--config",
        type=Path,
        default=Path("server_config.json"),
        help="server_config.json path (members: uuid, host, raft_port, tcp_port)",
    )
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
    p.add_argument("--run-test", action="append", default=[],
                    help="name of a test to run after attaching (repeatable); see --list-tests")
    p.add_argument("--list-tests", action="store_true", help="list available tests and exit")
    p.add_argument(
        "--verbose", type=int, default=2, help="2 = info, 1 = debug, 0 = trace"
    )
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
            logger.info(f"killed process(es) matching '{name}'")
        elif result.returncode == 1:
            logger.info(f"no process matching '{name}' found")
        else:
            logger.error(f"pkill for '{name}' failed: {result.stderr.decode().strip()}")


def run(args, members):
    vol_id = uuid.UUID(args.vol_id) if args.vol_id else None
    cluster = ClusterManager(args.tcp_srv_binary, args.config, members, args.verbose)
    registry.set_cluster(cluster)
    disk = None

    try:
        cluster.start_all()
        logger.info(f"waiting {args.startup_wait}s for servers to come up...")
        time.sleep(args.startup_wait)

        dead = cluster.any_dead()
        if dead:
            raise RuntimeError(f"servers failed to start: {dead}")
        logger.info("all server processes alive, proceeding to create_volume")

        vol = registry.volumes.create(members, vol_id=vol_id, capacity=args.capacity, lba_size=args.lba_size)
        logger.info(f"volume created, waiting {args.startup_wait}s before attaching client...")
        time.sleep(args.startup_wait)

        if args.craft_disk_binary:
            disk = CraftDisk(args.craft_disk_binary, args.config, vol, args.verbose)
            disk.start()
            registry.add_disk(str(vol.vol_id), disk)
            logger.info(f"{len(members)} server(s) running, {vol}, disk at {disk.device_path}, "
                        f"press Ctrl+C to stop")
        else:
            logger.info(f"{len(members)} server(s) running, {vol}, press Ctrl+C to stop")

        time.sleep(args.startup_wait)
        if args.run_test:
            for name in args.run_test:
                try:
                    run_test(name, disk.device_path)
                except TestNotFoundError as e:
                    logger.error(str(e))
                    raise

        while True:
            time.sleep(1)
            dead = cluster.any_dead()
            if dead:
                raise RuntimeError(f"server(s) died unexpectedly: {dead}")
            if disk is not None and not disk.is_alive():
                raise RuntimeError("ublkpp_disk exited unexpectedly")

    except KeyboardInterrupt:
        logger.info("Ctrl+C -- shutting down...")
        if disk is not None:
            disk.stop()
        cluster.shutdown()

    except Exception as e:
        logger.error(f"error: {e}")
        logger.error("leaving processes running for inspection. PIDs:")
        for uuid_, proc in cluster.procs.items():
            logger.error(f"  server {uuid_}: pid={proc.pid}")
        if disk is not None and disk.proc is not None:
            logger.error(f"  ublkpp_disk: pid={disk.proc.pid}")
        logger.error("re-run with --cleanup to kill them later.")
        sys.exit(1)


def main():
    args = parse_args()

    if args.list_tests:
        for name in list_tests():
            print(name)
        return

    if args.cleanup:
        cleanup_stray_processes()
        return

    members = load_members(args.config)
    run(args, members)


if __name__ == "__main__":
    main()
