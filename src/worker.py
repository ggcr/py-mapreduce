import os
import pickle

class Worker:
    def __init__(self, n: int, root: str):
        self.n = n
        self.root = root

    def map(self, M: int, chunk_path: str):
        # read chunk from `files/chunks/chunk_{chunk_id}`
        chunk = ""
        with open(chunk_path, 'rb') as fd:
            chunk = pickle.load(fd)
        # assign to bucket (1st char mod M)
        buckets = {}
        for word in chunk.split(' '):
            b_id = ord(word[0]) % M
            buckets[b_id] = buckets.get(b_id, []) + [word]

        # note: there are not race conditions because the bucket is unique for each map worker node

        # stdout to intermediate buckets
        for bucket_num, v in buckets.items():
            bucket_name = f"mr-{self.n}-{bucket_num}"
            bucket_path = os.path.join(self.root, bucket_name)
            with open(bucket_path, 'a') as fd:
                [fd.write(f"{word}\n") for word in v]
        

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

        # stdout to out bucket
        out_path = os.path.join(self.root, f"out-{m}")
        with open(out_path, 'w') as fd:
            [fd.write(f"{k} {v}\n") for k, v in res.items()]
