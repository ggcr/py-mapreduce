import os
import math
import time
import shutil
import multiprocessing
from typing import Callable

from src import utils
from src.worker import Worker

class Driver():
    def __init__(self, N: int, M: int):
        if N > 0:
            self.N = N 
        else:
            raise ValueError(f"N must be a positive signed integer, got N={N}")

        if M > 0:
            self.M = M 
        else:
            raise ValueError(f"M must be a positive signed integer, got M={M}")

        self.BUCKETS_PARENT_PATH = "files/intermediate/"
        self.REDUCE_PARENT_PATH = "files/out/"

    def reset_state(self) -> None:
        # Reset State: there should be a more clean way to do this, however
        # for subsequent runs with different params (N, M), for short texts
        # we may re-use previous buckets. For now I'll delete dirs beforehand.
        # (TODO): Look into a better way to manage state.
        def reset(PATH: str) -> None:
            if os.path.exists(PATH):
                shutil.rmtree(PATH)
            os.makedirs(PATH, exist_ok=True)

        reset(self.BUCKETS_PARENT_PATH)
        reset(self.REDUCE_PARENT_PATH)

    def map_worker(self, n: int, chunk: str) -> None:
        w = Worker(n, self.BUCKETS_PARENT_PATH)
        w.map(self.M, chunk)

    def reduce_worker(self, m: int, buckets: list[str]) -> None:
        w = Worker(m, self.REDUCE_PARENT_PATH)
        w.reduce(m, buckets)

    def spawn(self, num_t: int, data: list, target: Callable) -> None:
        threads = []
        for n in range(0, num_t):
            t = multiprocessing.Process(
                target=target,
                args=(n, data[n],)
            )
            threads.append(t)
            t.start()
        for t in threads: # wait until all threads done
            t.join()

    def split_chunks(self, FILES: list[str]) -> list[str]:
        # SPLIT FILES INTO N CHUNKS
        content = utils.readAllWords(FILES)
        chunk_size = len(content) // self.N
        chunks = [' '.join(content[i: i+chunk_size]) for i in range(0, len(content), chunk_size)]
        if len(chunks) > self.N:
            chunks[self.N-1] = ' '.join(chunks[self.N-1:])
        return chunks

    def accumulate_output(self) -> dict[str, int]:
        count = {}
        for m in range(0, self.M):
            out_bucket = os.path.join(self.REDUCE_PARENT_PATH, f"out-{m}")
            if os.path.exists(out_bucket):
                with open(out_bucket, 'r') as fd:
                    bufcont = fd.read().splitlines()
                    for record in bufcont:
                        k, v = record.split(' ')
                        count[k] = count.get(k, 0) + int(v)
        return count

    def run(self, FILES: list[str]) -> None:
        self.reset_state()

        # Map phase
        chunks = self.split_chunks(FILES)
        print(f"[MAP PHASE with {self.N} workers]")
        self.spawn(self.N, chunks, self.map_worker)

        # Reduce phase
        buckets = [[os.path.join(self.BUCKETS_PARENT_PATH, f"mr-{n}-{m}") for n in range(0, self.N)] for m in range(0, self.M)]
        print(f"[REDUCE PHASE with {self.M} workers]")
        self.spawn(self.M, buckets, self.reduce_worker)

        return self.accumulate_output()

    
