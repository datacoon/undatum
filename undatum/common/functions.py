
def get_dict_value(adict, key, prefix=None):
    if prefix is None:
        prefix = key.split('.')
    if len(prefix) == 1:
        return adict[prefix[0]]
    else:
        return get_dict_value(adict[prefix[0]], key, prefix=prefix[1:])

def get_dict_value_deep(adict, key, prefix=None, as_array=False, splitter='.'):
    """Used to get value from hierarhic dicts in python with params with dots as splitter"""
    if prefix is None:
        prefix = key.split(splitter)
    if len(prefix) == 1:
        if type(adict) == type({}):
            if not prefix[0] in adict.keys():
                return None
            if as_array:
                return [adict[prefix[0]], ]
            return adict[prefix[0]]
        elif type(adict) == type([]):
            if as_array:
                result = []
                for v in adict:
                    if prefix[0] in v.keys():
                        result.append(v[prefix[0]])
                return result
            else:
                if len(adict) > 0 and prefix[0] in adict[0].keys():
                    return adict[0][prefix[0]]
        return None
    else:
        if type(adict) == type({}):
            if prefix[0] in adict.keys():
                return get_dict_value_deep(adict[prefix[0]], key, prefix=prefix[1:], as_array=as_array)
        elif type(adict) == type([]):
            if as_array:
                result = []
                for v in adict:
                    res = get_dict_value_deep(v[prefix[0]], key, prefix=prefix[1:], as_array=as_array)
                    if res:
                        result.extend(res)
                return result
            else:
                return get_dict_value_deep(adict[0][prefix[0]], key, prefix=prefix[1:], as_array=as_array)
        return None
