import sys

from undatum.common.iterable import IterableData

FILENAMES = ['data.jsonl.gz', 'data.jsonl.xz', 'data.jsonl.zip', 'data.jsonl.bz2']

def run(filename, num = 100):
    for n in range(0, num):
        id = IterableData(filename, options={'format_in': 'jsonl', 'encoding': 'utf8'})
        print(filename)
        all = []
        for d in id.iter():
            all.append(d)
        print(len(all))
        id.close()


if __name__ == "__main__":
    run(sys.argv[1])
