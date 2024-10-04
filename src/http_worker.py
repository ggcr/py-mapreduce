import sys
import json
import argparse
from multiprocessing import Process
from http.server import BaseHTTPRequestHandler, HTTPServer

from src.worker import Worker

class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # read data from POST body req
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        payload = json.loads(post_data)

        # basic routing (nothing fancy)
        if self.path.endswith('/map'):
            w = Worker(payload['n'], payload['BUCKETS_PARENT_PATH'])
            w.map(payload['M'], payload['chunk'])
            pass
        elif self.path.endswith('/reduce'):
            w = Worker(payload['m'], payload['REDUCE_PARENT_PATH'])
            w.reduce(payload['m'], payload['buckets'])
            pass
        else:
            # We should send from the Driver some ALIVE calls to check which workers are already created
            pass

        # send OK if we are done
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'processed'}).encode('utf-8')) 

def run(w_id: int) -> None:
    port = 8000 + w_id
    url = f"http://localhost:{port}"
    httpd = HTTPServer(('', port), RequestHandler)
    print(f"[HTTP WORKER] Worker {w_id} running on {url}")
    httpd.serve_forever()
    
if __name__ == '__main__':
    # Usage: python3 -m src.http_worker -N 3 -M 4
    # Usage for the Driver: python3 -m src.http_worker -id <id>
    # Range of ports will be 8001:
    parser = argparse.ArgumentParser(description="HTTP Worker Request Handler.")
    parser.add_argument('-N',  type=int, default=None, required=False, help="Number of map tasks")
    parser.add_argument('-M',  type=int, default=None, required=False, help="Number of reduce tasks")
    parser.add_argument('-id', type=int, default=None, required=False, help="Worker id (port 8000 + id)")
    args = parser.parse_args()

    N = args.N
    M = args.M
    id = args.id
    w_ids = []
    if id is not None:
        w_ids = [int(id)]
    elif N is not None and M is not None:
        w_ids = [int(id) for id in range(1, max(N, M) + 1)]
    else:
        raise ValueError(f"Incorrect usage of HTTPWorker for args={args}.")

    threads = []
    for w_id in w_ids:
        t = Process(target=run, args=(w_id,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
