import pytest

from autosubmit.helpers.utils import strtobool


@pytest.mark.parametrize(
    'val,expected',
    [
        # yes
        ('y', 1),
        ('yes', 1),
        ('t', 1),
        ('true', 1),
        ('on', 1),
        ('1', 1),
        ('YES', 1),
        ('TrUE', 1),
        # no
        ('no', 0),
        ('n', 0),
        ('f', 0),
        ('F', 0),
        ('false', 0),
        ('off', 0),
        ('OFF', 0),
        ('0', 0),
        # invalid
        ('Yay', ValueError),
        ('Nay', ValueError),
        ('Nah', ValueError),
        ('2', ValueError),
    ]
)
def test_strtobool(val, expected):
    if expected is ValueError:
        with pytest.raises(expected):
            strtobool(val)
    else:
        assert expected == strtobool(val)
