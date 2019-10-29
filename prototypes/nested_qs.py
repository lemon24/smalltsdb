from collections.abc import Mapping
from collections.abc import Sequence


# dict flattening


def flatten_dict(d, dict=dict, max_depth=10):
    """{'a': {'b': 1, 'c': 2}} -> {'a.b': 1, 'a.c': 2}"""
    rv = dict()
    for k, v in d.items():
        if isinstance(v, Mapping):
            if max_depth <= 1:
                raise ValueError("reached max_depth 0")
            for ik, iv in flatten_dict(v, dict, max_depth - 1).items():
                rv[k + '.' + ik] = iv
        else:
            rv[k] = v
    return rv


def merge_dict(a, b):
    rv = dict()
    for k in set(a) | set(b):
        if k not in a:
            rv[k] = b[k]
        elif k not in b:
            rv[k] = a[k]
        else:
            rv[k] = merge_dict(a[k], b[k])
    return rv


def unflatten_dict(d, dict=dict, max_depth=10):
    """{'a.b': 1, 'a.c': 2} -> {'a': {'b': 1, 'c': 2}}"""
    rv = dict()
    for k, v in d.items():
        ks = k.split('.')
        if len(ks) == 1:
            rv[k] = v
        else:
            if max_depth <= 1:
                raise ValueError("reached max_depth 0")
            # TODO: this could look better, I guess
            # https://stackoverflow.com/a/6037657
            rv[ks[0]] = merge_dict(
                rv.get(ks[0], dict()),
                unflatten_dict({'.'.join(ks[1:]): v}, dict, max_depth - 1),
            )
    return rv


ud = {'1': {'b': {'0': 'c', '1': 'd'}}}
fd = {'1.b.0': 'c', '1.b.1': 'd'}

assert flatten_dict(ud) == fd
assert unflatten_dict(fd) == ud


ud = {'a': {'b': 1, 'c': 2}, 'd': [3, 4], 'e': 5}

fd = {'a.b': 1, 'a.c': 2, 'd': [3, 4], 'e': 5}

assert flatten_dict(ud) == flatten_dict(fd) == fd
assert unflatten_dict(fd) == unflatten_dict(ud) == ud


ud = {'a': {'b': 1, 'c': {'d': 2}}, 'e': {'f': 3}, 'g': 4}

fd = {'a.b': 1, 'a.c.d': 2, 'e.f': 3, 'g': 4}


assert flatten_dict(ud) == flatten_dict(fd) == fd
assert unflatten_dict(fd) == unflatten_dict(ud) == ud

flatten_dict(ud, max_depth=3)
unflatten_dict(fd, max_depth=3)

try:
    flatten_dict(ud, max_depth=2)
    raise Exception("should have raised ValueError")
except ValueError:
    pass

try:
    unflatten_dict(fd, max_depth=2)
    raise Exception("should have raised ValueError")
except ValueError:
    pass


# int keys to list and back


def int_keys_to_list(d, dict=dict, max_depth=10):
    if not isinstance(d, Mapping):
        return d

    if max_depth <= 0:
        raise ValueError("reached max_depth 0")

    d = dict((k, int_keys_to_list(v, dict, max_depth - 1)) for k, v in d.items())

    if not all(k.isascii() and k.isdecimal() for k in d):
        return d

    d = dict((int(k), v) for k, v in d.items())

    if min(d) != 0:
        raise ValueError("must start from 0")
    if max(d) > len(d) - 1:
        raise ValueError("must have no gaps")

    return [v for _, v in sorted(d.items())]


def list_to_int_keys(d, dict=dict, max_depth=10):
    # strings are also Sequences, so list it is
    if not isinstance(d, list) and not isinstance(d, Mapping):
        return d

    if not isinstance(d, Mapping):
        d = dict((str(i), e) for i, e in enumerate(d))

    if max_depth <= 0:
        raise ValueError("reached max_depth 0")

    d = dict((k, list_to_int_keys(v, dict, max_depth - 1)) for k, v in d.items())

    return d


ik = {'0': 'a', '1': {'b': {'0': 'c', '1': 'd'}, '1': 'e'}}
ld = ['a', {'b': ['c', 'd'], '1': 'e'}]

assert int_keys_to_list(ik) == int_keys_to_list(ld) == ld
assert list_to_int_keys(ld) == list_to_int_keys(ik) == ik

int_keys_to_list(ik, max_depth=3)
list_to_int_keys(ld, max_depth=3)

try:
    int_keys_to_list(ik, max_depth=2)
    raise Exception("should have raised ValueError")
except ValueError:
    pass

try:
    list_to_int_keys(ld, max_depth=2)
    raise Exception("should have raised ValueError")
except ValueError:
    pass

try:
    int_keys_to_list({'1': 'a'})
    raise Exception("should have raised ValueError")
except ValueError:
    pass

try:
    int_keys_to_list({'0': 'a', '2': 'b'})
    raise Exception("should have raised ValueError")
except ValueError:
    pass


# wrapping up


def flatten(d):
    return flatten_dict(list_to_int_keys(d))


def unflatten(d):
    return int_keys_to_list(unflatten_dict(d))


assert unflatten(flatten(ld)) == ld


if __name__ == '__main__':
    from pprint import pprint

    pprint(ld)
    pprint(flatten(ld))
