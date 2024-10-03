import os
import math
import time
import shutil
import multiprocessing

from src import utils
from src.main import sequential_count
from src.worker import Worker

# Naïve approach: spawn a worker for each file.
import sys

BUCKETS_PARENT_PATH = "files/intermediate/"
REDUCE_PARENT_PATH = "files/out/"

def reset_state():
    # Reset State: there should be a more clean way to do this, however
    # for subsequent runs with different params (N, M), for short texts
    # we may re-use previous buckets. For now I'll delete dirs beforehand.
    # (TODO): Look into a better way to manage state.
    if os.path.exists(BUCKETS_PARENT_PATH):
        shutil.rmtree(BUCKETS_PARENT_PATH)
    os.makedirs(BUCKETS_PARENT_PATH, exist_ok=True)

    if os.path.exists(REDUCE_PARENT_PATH):
        shutil.rmtree(REDUCE_PARENT_PATH)
    os.makedirs(REDUCE_PARENT_PATH, exist_ok=True)

def map_worker(n: int, M: int, chunk: str) -> None:
    print(f"[ID {n}, M {M}] CHUNK {chunk}")
    w = Worker(n, BUCKETS_PARENT_PATH)
    w.map(M, chunk)

def reduce_worker(m: int, buckets: list[str]):
    print(f"[ID {m}] {buckets}")
    w = Worker(m, REDUCE_PARENT_PATH)
    w.reduce(m, buckets)

def spawn_workers(N: int, M: int, FILES: list[str]) -> None:
    if N <= 0: raise ValueError(f"N must be a positive signed integer, got N={N}")
    if M <= 0: raise ValueError(f"M must be a positive signed integer, got M={M}")
    reset_state()

    # SPLIT FILES INTO N CHUNKS
    content = utils.readAllWords(FILES)
    chunk_size = len(content) // N
    chunks = [' '.join(content[i: i+chunk_size]) for i in range(0, len(content), chunk_size)]
    if len(chunks) > N:
        chunks[N-1] = ' '.join(chunks[N-1:])
    print(f"FILES: {FILES}")
    print(f"content: {content}")
    print(f"chunks (of size {chunk_size}): {chunks}")
    
    # MAP PHASE
    print(f"Starting {N} map workers")
    threads = []
    for n in range(0, N):
        t = multiprocessing.Process(
            target=map_worker,
            args=(n, M, chunks[n])
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
    N = 20 # map tasks
    M = 4 # reduce tasks

    spawn_workers(N, M, FILES)
    final_count = accumulate_output(M)
    print(final_count)

    GT_count = sequential_count(FILES)
    print(GT_count)
    assert final_count == GT_count, f"Expected {GT_count}, got {final_count}"
