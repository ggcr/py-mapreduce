import multiprocessing
import os
import time
from src import utils
# Naïve approach: spawn a worker for each file.

BUCKETS_PARENT_PATH = "files/intermediate/"

class Worker:
    def __init__(self, n: int, M: int, FILE_NAME: str):
        self.n = n
        self.M = M
        self.file_name = FILE_NAME

    def map(self):
        # read file
        contents = utils.readFile(self.file_name).split(' ')
        buckets = {m: [] for m in range(0, self.M)}
        for word in contents:
            buckets[ord(word[0]) % self.M].append(word)
        # write intermediary result to bucket
        os.makedirs(BUCKETS_PARENT_PATH, exist_ok=True)
        # (TODO): Serialize with pickle if needed.
        for bucket_num, v in buckets.items():
            bucket_path = os.path.join(BUCKETS_PARENT_PATH, f"m-{self.n}-{bucket_num}")
            with open(bucket_path, 'w') as fd:
                [fd.write(f"{word}\n") for word in v]

    def reduce(self):
        pass
        

def dummy_worker(n: int, M: int, filename: str):
    print(f"[ID {n}, M {M}] FILE {filename}")
    w = Worker(n, M, filename)
    w.map()

def spawn_workers(N: int, M: int) -> None:
    FILES = ["inputs/simple_test_1.txt", "inputs/simple_test_2.txt", "inputs/simple_test_3.txt",]
    threads = []
    for i in range(0, N):
        t = multiprocessing.Process(
            target=dummy_worker,
            args=(i, M, FILES[i])
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    print("Done")
    
if __name__ == '__main__':
    N = 3 # map tasks
    M = 5 # reduce tasks

    print(f"Starting {N} workers")
    spawn_workers(N, M)
