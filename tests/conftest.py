import os
import sys

import pytest

# Make `dummy_workloads` importable here and in spawned worker processes
# (multiprocessing "spawn" children inherit the parent's sys.path).
sys.path.insert(0, os.path.dirname(__file__))
_src = os.path.dirname(os.path.dirname(__file__))
if os.path.isdir(os.path.join(_src, "faketickets")):  # source checkout; in the image the installed package is used
    sys.path.insert(0, _src)


def pytest_collection_modifyitems(config, items):
    if os.environ.get("FT_IT_CONTACT_POINTS"):
        return
    skip = pytest.mark.skip(reason="set FT_IT_CONTACT_POINTS=host[:port] to run integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
