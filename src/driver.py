import multiprocessing
import os
import time
from src import utils
# Naïve approach: spawn a worker for each file.

BUCKETS_PARENT_PATH = "files/intermediate/"
os.makedirs(BUCKETS_PARENT_PATH, exist_ok=True)

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

        # write intermediary result to bucket
        # (TODO): Serialize with pickle (more efficent)?
        # note: there are not race conditions because the bucket is unique for each map worker node
        for bucket_num, v in buckets.items():
            bucket_path = os.path.join(BUCKETS_PARENT_PATH, f"m-{self.n}-{bucket_num}")
            with open(bucket_path, 'a') as fd:
                [fd.write(f"{word}\n") for word in v]

    def reduce(self):
        pass
        

def map_worker(n: int, M: int, filename: str) -> None:
    print(f"[ID {n}, M {M}] FILE {filename}")
    w = Worker(n)
    w.map(M, filename)

def reduce_worker(m: int, buckets: list[str]):
    print(f"[ID {m}] {buckets}")
    w = Worker(m)

def spawn_workers(N: int, M: int) -> None:
    # MAP PHASE
    print(f"Starting {N} map workers")
    FILES = ["inputs/simple_test_1.txt", "inputs/simple_test_2.txt", "inputs/simple_test_3.txt",]
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
    avail_buckets = [os.path.join(BUCKETS_PARENT_PATH, f"mr-{n}-{m}") for n in range(0, N) for m in range(0, M)]
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
    
if __name__ == '__main__':
    N = 3 # map tasks
    M = 5 # reduce tasks

    spawn_workers(N, M)
