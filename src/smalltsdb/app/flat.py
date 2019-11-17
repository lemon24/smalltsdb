from collections.abc import Mapping
from collections.abc import Sequence


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


def is_nonstring_sequence(o):
    return isinstance(o, Sequence) and not isinstance(o, (str, bytes))


def list_to_int_keys(d, dict=dict, max_depth=10):
    if not is_nonstring_sequence(d) and not isinstance(d, Mapping):
        return d

    if not isinstance(d, Mapping):
        d = dict((str(i), e) for i, e in enumerate(d))

    if max_depth <= 0:
        raise ValueError("reached max_depth 0")

    d = dict((k, list_to_int_keys(v, dict, max_depth - 1)) for k, v in d.items())

    return d


def flatten(d, max_depth=10):
    return flatten_dict(list_to_int_keys(d, max_depth=max_depth), max_depth=max_depth)


def unflatten(d, max_depth=10):
    return int_keys_to_list(unflatten_dict(d, max_depth=max_depth), max_depth=max_depth)
