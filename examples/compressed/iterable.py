import sys
import timeit
from undatum.common.iterable import IterableData

FILENAMES = ['data.jsonl.gz', 'data.jsonl.xz', 'data.jsonl.zip', 'data.jsonl.bz2', 'data.jsonl']

def iterate_filename(filename, num = 100):
    idata = IterableData(filename, options={'format_in': 'jsonl', 'encoding': 'utf8'})
#    all = []
    n = 0
    for d in idata.iter():
        n += 1
#        all.append(d)
    idata.close()

def run():
    for filename in FILENAMES:
        rep = timeit.repeat(lambda: iterate_filename(filename), globals=globals(), number=100)
        print(filename, rep)

if __name__ == "__main__":
    run()
