import sys
import json
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
            print(f"MAP RECIEVED \t payload = {payload}")
            w = Worker(payload['n'], payload['BUCKETS_PARENT_PATH'])
            w.map(payload['M'], payload['chunk'])
            pass
        elif self.path.endswith('/reduce'):
            print(f"REDUCE RECIEVED \t payload = {payload}")
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
    # Usage: python3 -m src.http_worker <id_1> <id_2> ... <id_N>
    threads = []
    for n_arg in range(1, len(sys.argv)):
        t = Process(target=run, args=(int(sys.argv[n_arg]),))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
