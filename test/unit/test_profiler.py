import pytest
from autosubmit.profiler.profiler import Profiler
from log.log import AutosubmitCritical

@pytest.fixture
def profiler():
    """ Creates a profiler object and yields it to the test. """
    yield Profiler("a000")

# Black box techniques for status machine based software
#
#   O--->__init__----> start
#                           |
#                           |
#                         stop (----> report) --->0

# Transition coverage
def test_transitions(profiler):
    # __init__ -> start
    profiler.start()

    # start -> stop
    profiler.stop()

def test_transitions_fail_cases(profiler):
    # __init__ -> stop
    with pytest.raises(AutosubmitCritical):
        profiler.stop()

    # start -> start
    profiler.start()
    with pytest.raises(AutosubmitCritical):
        profiler.start()

    # stop -> stop
    profiler.stop()
    with pytest.raises(AutosubmitCritical):
        profiler.stop()

# White box tests
def test_writing_permission_check_fails(profiler, mocker):
    mocker.patch("os.access", return_value=False)

    profiler.start()
    with pytest.raises(AutosubmitCritical):
        profiler.stop()

def test_memory_profiling_loop(profiler):
    profiler.start()
    bytearray(1024*1024)
    profiler.stop()
