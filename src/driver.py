import multiprocessing
import uuid
import time

def dummy_worker(id: uuid.UUID, seconds):
    print(f"[Worker with ID {str(id)}] Sleeping for {seconds} seconds")
    time.sleep(seconds)

def spawn_workers(n: int) -> None:
    threads = []
    for i in range(0, n):
        t = multiprocessing.Process(
            target=dummy_worker,
            args=(uuid.uuid4(), i+1,)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    print("Done")
    
if __name__ == '__main__':
    N = 5 # map tasks
    M = 5 # reduce tasks

    print(f"Starting {N} workers")
    spawn_workers(N)
