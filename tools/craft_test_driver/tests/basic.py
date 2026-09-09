import logging
import time

from fio_runner import run_fio

logger = logging.getLogger(__name__)


class FioError(RuntimeError):
    pass


def _timed(label: str, fn) -> None:
    t0 = time.monotonic()
    rc = fn()
    logger.info("%s took %.1fs", label, time.monotonic() - t0)
    if rc != 0:
        raise FioError(f"{label} failed, fio exit code={rc}")


def write_read_test(device: str) -> None:
    logger.info("=== basic write/read test on %s ===", device)
    _timed("write phase", lambda: run_fio(device, job_name="basic_write", rw="write",
                                          extra_args={"verify": "crc32c", "do_verify": "0"}))
    _timed("read/verify phase", lambda: run_fio(device, job_name="basic_verify", rw="read",
                                                extra_args={"verify": "crc32c", "do_verify": "1"}))
    logger.info("=== basic write/read test PASSED ===")


def mixed_readwrite_test(device: str, runtime: int = 30) -> None:
    logger.info("=== mixed read/write test on %s (runtime=%ds) ===", device, runtime)
    _timed("mixed read/write phase", lambda: run_fio(device, job_name="mixed_readwrite", size="500m", rw="randrw",
                                                     runtime=runtime,
                                                     extra_args={"verify": "crc32c", "do_verify": "1",
                                                                "rwmixread": "50"}))
    logger.info("=== mixed read/write test PASSED ===")


def run_all(device: str) -> None:
    logger.info("starting basic tests")
    write_read_test(device)
    mixed_readwrite_test(device)