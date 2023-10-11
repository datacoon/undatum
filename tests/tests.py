# -*- coding: utf8 -*-
## Very unfinished, just a few example for future full rewrite to conform pytest

from copy import deepcopy
def strip_dict_fields(record, fields, startkey=0):
    keys = record.keys()
    localf = []
    for field in fields:
        if len(field) > startkey:
            localf.append(field[startkey])
    for k in list(keys):
        if k not in localf:
            del record[k]
                        
    if len(k) > 0:      
        for k in record.keys():
            if type(record[k]) == type({}):
                record[k] = strip_dict_fields(record[k], fields, startkey+1)
    return record


thedict = {'clear' : 1, 'clean' : {'subclean' : 5, 'subsubclean' : "ready!", "open" : {'subopen' : 'notsoopen'}}}

print(thedict)
print(strip_dict_fields(deepcopy(thedict), fields=[['clean', 'subclean']]))
print(thedict)
print(strip_dict_fields(deepcopy(thedict), fields=[['clear',]]))
print(thedict)
print(strip_dict_fields(deepcopy(thedict), fields=[['clear',], ['open', 'subopen']]))
print(thedict)
print(strip_dict_fields(deepcopy(thedict), fields=[['clear',], ['clean', 'subsubclean']]))
