import os

def readFile(path: str) -> str:
    try:
        res = ""
        with open(path, 'r') as fd:
            res = fd.read()
            res = res.strip() # remove \n
        return res
    except FileNotFoundError:
        print(f"File with path {path} was not found.")
    return None

def map(filename: str, contents: str) -> dict[str, int]:
    res = {}
    if contents is not None:
        for word in contents.split(' '):
            res[word] = res.get(word, 0) + 1
    return res

def reduce(maps: list[dict[str, int]]) -> dict[str, int]:
    res = {}
    for map in maps:
        for k, v in map.items():
            res[k] = res.get(k, 0) + v
    return res

if __name__ == '__main__':

    # TEST CASE 1: Normal test case, 1 file, sequential
    FILE_PATH = "inputs/simple_test_1.txt"
    map1 = map(FILE_PATH, readFile(FILE_PATH))
    gt1 = {'The': 1, 'quick': 1, 'brown': 1, 'fox': 1, 'jumps': 1, 'over': 1, 'the': 1, 'lazy': 1, 'dog.': 1}
    assert map1 == gt1, f"Expected {gt1}, got {map1}"

    # TEST CASE 2: Non-existent FILE_PATH
    FILE_PATH = "/tmp/thisPathDoesNotExist/path.txt"
    map1 = map(FILE_PATH, readFile(FILE_PATH))
    gt1 = {}
    assert map1 == gt1, f"Expected {gt1}, got {map1}"
    assert os.path.exists(FILE_PATH) == False, f"File {FILE_PATH} should not be found, but was found."

    # TEST CASE 3: Map-Reduce single file sequential
    FILE_PATH = "inputs/simple_test_1.txt"
    map1 = map(FILE_PATH, readFile(FILE_PATH))
    reduce1 = reduce([map1])
    gt1 = {'The': 1, 'quick': 1, 'brown': 1, 'fox': 1, 'jumps': 1, 'over': 1, 'the': 1, 'lazy': 1, 'dog.': 1}
    assert map1 == gt1, f"Expected {gt1}, got {map1}"
    assert reduce1 == gt1, f"Expected {gt1}, got {reduce1}"

    # TEST CASE 4: Map-Reduce multiple files sequential
    FILE_PATH_1 = "inputs/simple_test_1.txt"
    FILE_PATH_2 = "inputs/simple_test_2.txt"
    FILE_PATH_3 = "inputs/simple_test_3.txt"
    map1 = map(FILE_PATH_1, readFile(FILE_PATH_1))
    map2 = map(FILE_PATH_2, readFile(FILE_PATH_2))
    map3 = map(FILE_PATH_3, readFile(FILE_PATH_3))
    reduce1 = reduce([map1, map2, map3])
    gt1 = {'The': 1, 'quick': 1, 'brown': 1, 'fox': 1, 'jumps': 1, 'over': 1, 'the': 1, 'lazy': 1, 'dog.': 1}
    gt2 = {'The': 1, 'quick': 1, 'fox': 1, 'is': 1, 'orange.': 1}
    gt3 = {'The': 1, 'fox': 1, 'jumps': 1, 'high': 1, 'over': 1, 'the': 1, 'lazy': 1, 'dog.': 1}
    gt  = {'The': 3, 'quick': 2, 'brown': 1, 'fox': 3, 'jumps': 2, 'over': 2, 'the': 2, 'lazy': 2, 'dog.': 2, 'is': 1, 'orange.': 1, 'high': 1}
    assert map1 == gt1, f"Expected {gt1}, got {map1}"
    assert map2 == gt2, f"Expected {gt2}, got {map2}"
    assert map3 == gt3, f"Expected {gt3}, got {map3}"
    assert reduce1 == gt, f"Expected {gt}, got {reduce1}"
