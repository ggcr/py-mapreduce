import re

def readFile(path: str) -> str:
    try:
        res = ""
        with open(path, 'r') as fd:
            res = fd.read()
            # remove non-alpha chars from word
            res = re.sub('[^a-zA-Z0-9]+', ' ', res).strip() # punctuations
            res = re.sub('[^a-zA-Z0-9]*$', '', res).lower() # special chars at the end
        return res
    except FileNotFoundError:
        print(f"File with path {path} was not found.")
    return None

def readAllWords(FILES: list[str]) -> list[str]:
    content = []
    for file in FILES:
        content.extend(readFile(file).split(' '))
    print(content)
    return content
