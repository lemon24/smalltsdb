import pytest

from smalltsdb.app.flat import flatten
from smalltsdb.app.flat import flatten_dict
from smalltsdb.app.flat import int_keys_to_list
from smalltsdb.app.flat import list_to_int_keys
from smalltsdb.app.flat import unflatten
from smalltsdb.app.flat import unflatten_dict


def test_flatten_unflatten_dict():
    # TODO: parametrize, maybe

    ud = {'1': {'b': {'0': 'c', '1': 'd'}}}
    fd = {'1.b.0': 'c', '1.b.1': 'd'}

    assert flatten_dict(ud) == flatten_dict(fd) == fd
    assert unflatten_dict(fd) == unflatten_dict(ud) == ud

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

    with pytest.raises(ValueError):
        flatten_dict(ud, max_depth=2)

    with pytest.raises(ValueError):
        unflatten_dict(fd, max_depth=2)


def test_int_keys():
    ik = {'0': 'a', '1': {'b': {'0': 'c', '1': 'd'}, '1': 'e'}}
    ld = ['a', {'b': ['c', 'd'], '1': 'e'}]

    assert int_keys_to_list(ik) == int_keys_to_list(ld) == ld
    assert list_to_int_keys(ld) == list_to_int_keys(ik) == ik

    int_keys_to_list(ik, max_depth=3)
    list_to_int_keys(ld, max_depth=3)

    with pytest.raises(ValueError):
        int_keys_to_list(ik, max_depth=2)

    with pytest.raises(ValueError):
        list_to_int_keys(ld, max_depth=2)
        raise Exception("should have raised ValueError")

    with pytest.raises(ValueError):
        int_keys_to_list({'1': 'a'})

    with pytest.raises(ValueError):
        int_keys_to_list({'0': 'a', '2': 'b'})


def test_flatten_unflatten():
    ud = {'a': [{'x': 1}, {'x': 2, 'y': 3}], 'b': {'c': ['foo', 'bar'], 'd': 4}, 'e': 5}
    fd = {
        'a.0.x': 1,
        'a.1.x': 2,
        'a.1.y': 3,
        'b.c.0': 'foo',
        'b.c.1': 'bar',
        'b.d': 4,
        'e': 5,
    }

    assert flatten(ud) == flatten(fd) == fd
    assert unflatten(fd) == unflatten(ud) == ud
