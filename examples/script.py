def process(item):
    item['name'] = item['title']
    del item['title']
    return item