import os
import sys
import math
import time
import pickle
import shutil
import requests
import subprocess
import multiprocessing
from typing import Callable

from src import utils
from src.worker import Worker

class Driver():
    def __init__(self, N: int, M: int):
        if N <= 0 or M <= 0: raise ValueError(f"N and M must be a positive signed integer, got N={N} and M={M}.")
        self.N = N 
        self.M = M 
        self.BUCKETS_PARENT_PATH = "files/intermediate/"
        self.REDUCE_PARENT_PATH = "files/out/"
        self.CHUNKS_PATH = "files/chunks/"
        self.WORKERS_URL = "http://localhost:8000/worker"

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
        reset(self.CHUNKS_PATH)

    def map_worker(self, n: int, chunk: str, retry: int = 0) -> None:
        try:
            payload = {
                'n': n, 'M': self.M, 'chunk': chunk,
                'BUCKETS_PARENT_PATH': self.BUCKETS_PARENT_PATH,
            }
            response = requests.post(url=f"{self.WORKERS_URL}_{8000 + n}/map", json=payload)
            response.raise_for_status() # checks that res.status_code == 200 (OK)
        except requests.exceptions.ConnectionError:
            # http worker is down
            # we can raise it up from here :)
            process = subprocess.Popen([sys.executable, '-m', 'src.http_worker', str(n)])
            # recursively try to reconnect 4 more times
            if retries < 4:
                self.map_worker(m, buckets, retries + 1)


    def reduce_worker(self, m: int, buckets: list[str], retries: int = 0) -> None:
        try:
            payload = {
                'm': m,
                'REDUCE_PARENT_PATH': self.REDUCE_PARENT_PATH,
                'buckets': buckets
            }
            response = requests.post(url=f"{self.WORKERS_URL}_{m}/reduce", json=payload)
            response.raise_for_status() # checks that res.status_code == 200 (OK)
        except requests.exceptions.ConnectionError:
            # http worker is down
            # we can raise it up from here :)
            process = subprocess.Popen([sys.executable, '-m', 'src.http_worker', str(m)])
            # recursively try to reconnect 4 more times
            if retries < 4:
                self.reduce_worker(m, buckets, retries + 1)

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
        # serialize chunks at `files/chunks/chunk_{i}.pkl`
        chunks_ids = []
        for i in range(0, len(chunks)):
            chunk_store_path = os.path.join(self.CHUNKS_PATH, f'chunk_{i}.pkl')
            with open(chunk_store_path, 'wb') as fd:
                pickle.dump(chunks[i], fd)
                chunks_ids.append(chunk_store_path)
        return chunks_ids

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
        chunks_paths = self.split_chunks(FILES)
        print(f"[MAP PHASE with {self.N} workers]")
        self.spawn(self.N, chunks_paths, self.map_worker)

        # Reduce phase
        buckets_paths = [[os.path.join(self.BUCKETS_PARENT_PATH, f"mr-{n}-{m}") for n in range(0, self.N)] for m in range(0, self.M)]
        print(f"[REDUCE PHASE with {self.M} workers]")
        self.spawn(self.M, buckets_paths, self.reduce_worker)

        return self.accumulate_output()

    
