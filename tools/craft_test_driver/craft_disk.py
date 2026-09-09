import logging
import re
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

from volumes import Volume
import registry

logger = logging.getLogger(__name__)
log_level_dict: dict[int, str] = {0: "trace", 1: "debug", 2: "info"}

class CraftDisk:
    """Owns one ublkpp_disk --craft_tcp subprocess attaching to a Volume. Parses the resulting
    /dev/ublkbN path from stdout once ublkpp_tgt::run() reports the device exposed."""

    _DEVICE_RE = re.compile(r"exposed as UBD device: \[(/dev/ublkb\d+)\]")
    _LOG_PATH = Path("logs/latest/ublkpp_disk_log")

    def __init__(
        self,
        binary: Path,
        config_path: Path,
        vol: Volume,
        verbose: int,
        ready_timeout: float = 15.0,
    ):
        self.binary = binary
        self.config_path = config_path
        self.vol = vol
        self.ready_timeout = ready_timeout
        lvl = log_level_dict[verbose]
        self.log_args = f"base:{lvl},ublksrv:{lvl},ublk_tgt:{lvl},ublk_raid:{lvl},ublk_drivers:{lvl}"
        self.proc: subprocess.Popen | None = None
        self.device_path: str | None = None

    def start(self):
        cmd = [
            "stdbuf",
            "-oL",
            str(self.binary),
            "--craft_tcp",
            "--server_config_file",
            str(self.config_path),
            "--vol_id",
            str(self.vol.vol_id),
            "--log_mods",
            str(self.log_args),
        ]
        logger.info("starting: %s", " ".join(cmd))
        # stdout/stderr -> DEVNULL: ublkpp_disk's async (spdlog) logger writes to its own log file
        # regardless; leaving stdout as an unread PIPE risks that logger blocking forever once the
        # OS pipe buffer fills.
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._wait_ready()

    def _wait_ready(self):
            deadline = time.monotonic() + self.ready_timeout
            # The log file may not exist yet the instant the process starts -- wait for it too.
            while not self._LOG_PATH.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"ublkpp_disk log file never appeared within {self.ready_timeout}s")
                if (rc := self.proc.poll()) is not None:
                    raise RuntimeError(f"ublkpp_disk exited early (code={rc}) before creating a log file")
                time.sleep(0.05)
    
            with open(self._LOG_PATH) as f:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(f"ublkpp_disk did not expose a device within {self.ready_timeout}s")
                    if (rc := self.proc.poll()) is not None:
                        if rc < 0:
                            raise RuntimeError(f"ublkpp_disk CRASHED (signal {-rc})")
                        raise RuntimeError(f"ublkpp_disk exited early (code={rc}) before exposing a device")
                    if registry.cluster is not None:
                        dead = registry.cluster.any_dead()
                        if dead:
                            raise RuntimeError(f"server(s) died while waiting for disk attach: {dead}")
    
                    line = f.readline()
                    if not line:
                        time.sleep(0.1)
                        continue
                    m = self._DEVICE_RE.search(line)
                    if m:
                        self.device_path = m.group(1)
                        logger.info("ublkpp_disk exposed device: %s", self.device_path)
                        return
    
    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def stop(self):
        if self.proc is None or self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("ublkpp_disk did not exit on SIGTERM, killing")
            self.proc.kill()
            self.proc.wait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
