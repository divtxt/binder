
import platform


def get_assert_tuple_args(e):
    assert isinstance(e, AssertionError)
    args = e.args
    assert isinstance(args, tuple)
    if platform.python_version_tuple() < ('2','7','3'):
        # assert tuple args already unpacked!
        # see http://bugs.python.org/issue13268
        return args
    else:
        assert len(args) == 1
        return args[0]

