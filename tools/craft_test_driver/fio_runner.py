import logging
import subprocess

logger = logging.getLogger(__name__)

DEFAULT_SIZE = "64m"
DEFAULT_BS = "4k"
DEFAULT_IOENGINE = "io_uring"


class FioError(RuntimeError):
    pass


def run_fio(device: str, job_name: str, rw: str, size: str = DEFAULT_SIZE, bs: str = DEFAULT_BS,
           ioengine: str = DEFAULT_IOENGINE, direct: bool = True, runtime: int = None,
           extra_args: dict[str, str] = None) -> int:
    """Run one fio job against `device`, streaming its normal terminal output live. Returns fio's
    exit code (0 = success)."""
    args = [
        "fio",
        f"--name={job_name}",
        f"--filename={device}",
        f"--rw={rw}",
        f"--bs={bs}",
        f"--size={size}",
        f"--ioengine={ioengine}",
        f"--direct={1 if direct else 0}",
        "--status-interval=1",
    ]
    if runtime is not None:
        args.append(f"--runtime={runtime}")
        args.append("--time_based")
    for k, v in (extra_args or {}).items():
        args.append(f"--{k}={v}")

    logger.info("running: %s", " ".join(args))
    proc = subprocess.run(args)
    return proc.returncode