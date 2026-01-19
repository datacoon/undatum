import timeit
from iterable.helpers.detect import open_iterable

FILENAMES = ['data.jsonl.gz', 'data.jsonl.xz', 'data.jsonl.zip', 'data.jsonl.bz2', 'data.jsonl']

def iterate_filename(filename, num = 100):
    iterable = open_iterable(filename, mode='r', iterableargs={'format_in': 'jsonl', 'encoding': 'utf8'})
    try:
        n = 0
        for d in iterable:
            n += 1
    finally:
        iterable.close()

def run():
    for filename in FILENAMES:
        rep = timeit.repeat(lambda: iterate_filename(filename), globals=globals(), number=100)
        print(filename, rep)

if __name__ == "__main__":
    run()
