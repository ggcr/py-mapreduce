import multiprocessing
import os
import time
from src import utils
from src.main import sequential_count
# Naïve approach: spawn a worker for each file.

BUCKETS_PARENT_PATH = "files/intermediate/"
os.makedirs(BUCKETS_PARENT_PATH, exist_ok=True)

REDUCE_PARENT_PATH = "files/out/"
os.makedirs(REDUCE_PARENT_PATH, exist_ok=True)

class Worker:
    def __init__(self, n: int):
        self.n = n

    def map(self, M: int, FILENAME: str):
        # read file
        contents = utils.readFile(FILENAME).split(' ')
        buckets = {}
        for word in contents:
            b_id = ord(word[0]) % M
            buckets[b_id] = buckets.get(b_id, []) + [word]

        # create bucket files
        # BUCKETS_PARENT_PATH = "files/intermediate/"
        # os.makedirs(BUCKETS_PARENT_PATH, exist_ok=True)
        # [open(os.path.join(BUCKETS_PARENT_PATH, f"mr-{n}-{m}"), 'a').close() for m in range(0, M) for n in range(0, self.n)]

        # write intermediate result to bucket
        # (TODO): Serialize with pickle (more efficent)?
        # note: there are not race conditions because the bucket is unique for each map worker node

        # This write flag will overwrite previous executions runs results
        overwrite = {bucket_num: True for bucket_num in set(buckets.keys())} 
        for bucket_num, v in buckets.items():
            bucket_name = f"mr-{self.n}-{bucket_num}"
            bucket_path = os.path.join(BUCKETS_PARENT_PATH, bucket_name)
            with open(bucket_path, 'w' if overwrite[bucket_num] else 'a') as fd:
                [fd.write(f"{word}\n") for word in v]
            overwrite[bucket_num] = False


    def reduce(self, m: int, buckets: list[str]):
        # count words from each reduce bucket
        res = {}
        for bucket in buckets:
            data = []
            if os.path.exists(bucket):
                with open(bucket, 'r') as fd:
                    data = fd.read().splitlines() 
                for word in data:
                    res[word] = res.get(word, 0) + 1
        print(res)
        # output to file
        out_path = os.path.join(REDUCE_PARENT_PATH, f"out-{m}")
        with open(out_path, 'w') as fd:
            [fd.write(f"{k} {v}\n") for k, v in res.items()]

def map_worker(n: int, M: int, filename: str) -> None:
    print(f"[ID {n}, M {M}] FILE {filename}")
    w = Worker(n)
    w.map(M, filename)

def reduce_worker(m: int, buckets: list[str]):
    print(f"[ID {m}] {buckets}")
    w = Worker(m)
    w.reduce(m, buckets)

def spawn_workers(N: int, M: int, FILES: list[str]) -> None:
    # MAP PHASE
    print(f"Starting {N} map workers")
    N = len(FILES) # provisional
    threads = []
    for n in range(0, N):
        t = multiprocessing.Process(
            target=map_worker,
            args=(n, M, FILES[n])
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    print("Map phase done!\n")

    # REDUCE PHASE
    print(f"Starting {M} reduce workers")
    for m in range(0, M):
        avail_buckets = [os.path.join(BUCKETS_PARENT_PATH, f"mr-{n}-{m}") for n in range(0, N)]
        t = multiprocessing.Process(
            target=reduce_worker,
            args=(m, avail_buckets, )
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    print("Reduce phase done!\n")

def accumulate_output(M: int):
    count = {}
    for m in range(0, M):
        out_bucket = os.path.join(REDUCE_PARENT_PATH, f"out-{m}")
        if os.path.exists(out_bucket):
            with open(out_bucket, 'r') as fd:
                bufcont = fd.read().splitlines()
                for record in bufcont:
                    k, v = record.split(' ')
                    count[k] = count.get(k, 0) + int(v)
    return count
    
if __name__ == '__main__':
    FILES = ["inputs/simple_test_1.txt", "inputs/simple_test_2.txt", "inputs/simple_test_3.txt",]
    N = 3 # map tasks
    M = 5 # reduce tasks

    spawn_workers(N, M, FILES)
    final_count = accumulate_output(M)
    print(final_count)

    GT_count = sequential_count(FILES)
    print(GT_count)
    assert final_count == GT_count, f"Expected {GT_count}, got {final_count}"
