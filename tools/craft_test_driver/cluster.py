# cluster.py
import signal
import subprocess
import sys
from pathlib import Path

log_level_dict: dict[int, str] = {0: "trace", 1: "debug", 2: "info"}

class ClusterManager:
    """Owns a set of craft_reference_tcp_srv subprocesses. Guarantees cleanup on exit, even if
    the script is interrupted or a later step raises."""

    def __init__(
        self, binary: Path, config_path: Path, members: list[dict], verbose: int
    ):
        self.binary = binary
        self.config_path = config_path
        self.members = members
        lvl = log_level_dict[verbose]
        self.log_args = f"base:{lvl},nuraft_mesg:info"
        self.procs: dict[str, subprocess.Popen] = {}

    def start_all(self):
        for m in self.members:
            self._start_one(m)

    def _start_one(self, member: dict):
        uuid_ = member["uuid"]
        port = member["tcp_port"]
        cmd = [
            str(self.binary),
            "--port",
            str(port),
            "--server_config_file",
            str(self.config_path),
            "--server_uuid",
            uuid_,
            "--log_mods",
            str(self.log_args),
        ]
        print(f"starting: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self.procs[uuid_] = proc

    def any_dead(self) -> list[str]:
        return [u for u, p in self.procs.items() if p.poll() is not None]

    def shutdown(self):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for uuid_, proc in self.procs.items():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"server {uuid_} did not exit on SIGTERM, killing", file=sys.stderr)
                proc.kill()
                proc.wait()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.shutdown()
