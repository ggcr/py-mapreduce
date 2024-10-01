import re

def readFile(path: str) -> str:
    try:
        res = ""
        with open(path, 'r') as fd:
            res = fd.read()
            # remove non-alpha chars from word
            res = re.sub('[^a-zA-Z0-9]*$', '', res)
        return res
    except FileNotFoundError:
        print(f"File with path {path} was not found.")
    return None
