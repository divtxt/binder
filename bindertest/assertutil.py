
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

# copy/paste for use in pre-2.7
_MAX_LENGTH = 80
def safe_repr(obj, short=False):
    try:
        result = repr(obj)
    except Exception:
        result = object.__repr__(obj)
    if not short or len(result) < _MAX_LENGTH:
        return result
    return result[:_MAX_LENGTH] + ' [truncated]...'
def assertIn(self, member, container, msg=None):
    """Just like self.assertTrue(a in b), but with a nicer default message."""
    if member not in container:
        standardMsg = '%s not found in %s' % (safe_repr(member),
                                              safe_repr(container))
        self.fail(self._formatMessage(msg, standardMsg))
