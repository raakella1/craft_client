import importlib
import logging

logger = logging.getLogger(__name__)

TESTS = {
    "basic": "tests.basic",
}


class TestNotFoundError(RuntimeError):
    pass


def list_tests() -> list[str]:
    return list(TESTS.keys())


def run_test(name: str, device: str) -> None:
    if name not in TESTS:
        raise TestNotFoundError(f"unknown test '{name}'; available: {list_tests()}")
    module = importlib.import_module(TESTS[name])
    logger.info("=== running test: %s ===", name)
    module.run_all(device)
    logger.info("=== test PASSED: %s ===", name)