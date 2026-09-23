import os
import sys

import pytest

pytestmark = pytest.mark.no_backend_parametrization

from metaflow_extensions.nflx.plugins.functions.environment import run_in_path


def test_removes_the_entry_it_added(tmp_path):
    before = list(sys.path)

    run_in_path(lambda: None, str(tmp_path))

    assert sys.path == before


def test_removes_the_entry_when_the_loader_raises(tmp_path):
    before = list(sys.path)

    def boom():
        raise RuntimeError("loader failed")

    with pytest.raises(RuntimeError, match="loader failed"):
        run_in_path(boom, str(tmp_path))

    assert sys.path == before


def test_restores_the_working_directory(tmp_path):
    before = os.getcwd()

    run_in_path(lambda: None, str(tmp_path))

    assert os.getcwd() == before


def test_leaves_a_concurrently_added_entry_alone(tmp_path):
    """The bug: restoring a snapshot taken at entry drops anything added since.

    A second loader running while this one is open -- or anything else that
    touches sys.path -- loses the entry it is importing through.
    """
    other = str(tmp_path / "somebody-elses-path")

    run_in_path(lambda: sys.path.insert(0, other), str(tmp_path))

    try:
        assert other in sys.path
    finally:
        while other in sys.path:
            sys.path.remove(other)


def test_tolerates_the_loader_removing_the_entry_itself(tmp_path):
    root = str(tmp_path)

    run_in_path(lambda: sys.path.remove(root), root)

    assert root not in sys.path


def test_leaves_a_pre_existing_copy_of_the_same_directory_in_place(tmp_path):
    """A persistent holder of the same directory keeps its entry.

    This is the case a warm-start cache depends on: it inserts the function
    root once and expects later loads of that same root not to take it away.
    """
    root = str(tmp_path)
    sys.path.insert(0, root)
    try:
        run_in_path(lambda: None, root)

        assert sys.path.count(root) == 1
    finally:
        while root in sys.path:
            sys.path.remove(root)
