# craft_disk.py
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

from volumes import Volume


class CraftDisk:
    """Owns one ublkpp_disk --craft_tcp subprocess attaching to a Volume. Parses the resulting
    /dev/ublkbN path from stdout once ublkpp_tgt::run() reports the device exposed."""

    _DEVICE_RE = re.compile(r"exposed as UBD device: \[(/dev/ublkb\d+)\]")

    def __init__(self, binary: Path, vol: Volume, ready_timeout: float = 15.0):
        self.binary = binary
        self.vol = vol
        self.ready_timeout = ready_timeout
        self.proc: subprocess.Popen | None = None
        self.device_path: str | None = None

    def start(self):
        endpoints = ",".join(f"{m['host']}:{m['tcp_port']}" for m in self.vol.members)
        cmd = [
            str(self.binary),
            "--craft_tcp", endpoints,
            "--vol_id", str(self.vol.vol_id),
        ]
        print(f"starting: {' '.join(cmd)}")
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     text=True, bufsize=1, start_new_session=True)
        self._wait_ready()

    def _wait_ready(self):
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"ublkpp_disk exited early (code={self.proc.returncode}) "
                                   f"before exposing a device -- check its output above")
            line = self.proc.stdout.readline()
            if not line:
                continue
            print(f"[ublkpp_disk] {line.rstrip()}")
            m = self._DEVICE_RE.search(line)
            if m:
                self.device_path = m.group(1)
                return
        raise TimeoutError(f"ublkpp_disk did not expose a device within {self.ready_timeout}s")

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def stop(self):
        if self.proc is None or self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("ublkpp_disk did not exit on SIGTERM, killing", file=sys.stderr)
            self.proc.kill()
            self.proc.wait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()