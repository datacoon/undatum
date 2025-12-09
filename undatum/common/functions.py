# -*- coding: utf8 -*-
"""Common utility functions for dictionary operations."""
def get_dict_value(adict, key, prefix=None):
    """Get value from dictionary using dot-notation key."""
    if prefix is None:
        prefix = key.split('.')
    if len(prefix) == 1:
        return adict[prefix[0]]
    return get_dict_value(adict[prefix[0]], key, prefix=prefix[1:])


def get_dict_value_deep(adict, key, prefix=None, as_array=False, splitter='.'):
    """Get value from hierarchical dicts in python with params with dots as splitter."""
    if prefix is None:
        prefix = key.split(splitter)
    if len(prefix) == 1:
        if isinstance(adict, dict):
            if prefix[0] not in adict:
                return None
            if as_array:
                return [adict[prefix[0]]]
            return adict[prefix[0]]
        if isinstance(adict, list):
            if as_array:
                result = []
                for v in adict:
                    if prefix[0] in v:
                        result.append(v[prefix[0]])
                return result
            if len(adict) > 0 and prefix[0] in adict[0]:
                return adict[0][prefix[0]]
        return None
    if isinstance(adict, dict):
        if prefix[0] in adict:
            return get_dict_value_deep(adict[prefix[0]], key, prefix=prefix[1:],
                                       as_array=as_array)
    elif isinstance(adict, list):
        if as_array:
            result = []
            for v in adict:
                res = get_dict_value_deep(v[prefix[0]], key, prefix=prefix[1:],
                                          as_array=as_array)
                if res:
                    result.extend(res)
            return result
        return get_dict_value_deep(adict[0][prefix[0]], key, prefix=prefix[1:],
                                    as_array=as_array)
    return None
