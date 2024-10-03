import os
from src import utils


def sequential_count(FILES: list[str]):
    res = {}
    for i in range(0, len(FILES)):
        cont = utils.readFile(FILES[i])
        for word in cont.split(' '):
            res[word] = res.get(word, 0) + 1
    return res
        
if __name__ == '__main__':
    FILES = ["inputs/simple_test_1.txt", "inputs/simple_test_2.txt", "inputs/simple_test_3.txt"]
    count = sequential_count(FILES)
    print("FILES:")
    [print(file) for file in FILES]
    print("SEQUENTIAL COUNT:")
    print(count, '\n')

    # FILES = ['inputs/pg-sherlock_holmes.txt', 'inputs/pg-tom_sawyer.txt', 'inputs/pg-frankenstein.txt', 'inputs/pg-grimm.txt', 'inputs/pg-being_ernest.txt', 'inputs/pg-huckleberry_finn.txt', 'inputs/pg-metamorphosis.txt', 'inputs/pg-dorian_gray.txt']
    # count = sequential_count(FILES)
    # print("FILES:")
    # [print(file) for file in FILES]
    # print("SEQUENTIAL COUNT:")
    # print(count, '\n')

# def map(filename: str, contents: str) -> dict[str, int]:
#     res = {}
#     if contents is not None:
#         for word in contents.split(' '):
#             res[word] = res.get(word, 0) + 1
#     return res

# def reduce(maps: list[dict[str, int]]) -> dict[str, int]:
#     res = {}
#     for map in maps:
#         for k, v in map.items():
#             res[k] = res.get(k, 0) + v
#     return res

# if __name__ == '__main__':

#     # TEST CASE 1: Normal test case, 1 file, sequential
#     FILE_PATH = "inputs/simple_test_1.txt"
#     map1 = map(FILE_PATH, readFile(FILE_PATH))
#     gt1 = {'quick': 1, 'brown': 1, 'fox': 1, 'jumps': 1, 'over': 1, 'the': 2, 'lazy': 1, 'dog': 1}
#     assert map1 == gt1, f"Expected {gt1}, got {map1}"

#     # TEST CASE 2: Non-existent FILE_PATH
#     FILE_PATH = "/tmp/thisPathDoesNotExist/path.txt"
#     map1 = map(FILE_PATH, readFile(FILE_PATH))
#     gt1 = {}
#     assert map1 == gt1, f"Expected {gt1}, got {map1}"
#     assert os.path.exists(FILE_PATH) == False, f"File {FILE_PATH} should not be found, but was found."

#     # TEST CASE 3: Map-Reduce single file sequential
#     FILE_PATH = "inputs/simple_test_1.txt"
#     map1 = map(FILE_PATH, readFile(FILE_PATH))
#     reduce1 = reduce([map1])
#     gt1 = {'quick': 1, 'brown': 1, 'fox': 1, 'jumps': 1, 'over': 1, 'the': 2, 'lazy': 1, 'dog': 1}
#     assert map1 == gt1, f"Expected {gt1}, got {map1}"
#     assert reduce1 == gt1, f"Expected {gt1}, got {reduce1}"

#     # TEST CASE 4: Map-Reduce multiple files sequential
#     FILE_PATH_1 = "inputs/simple_test_1.txt"
#     FILE_PATH_2 = "inputs/simple_test_2.txt"
#     FILE_PATH_3 = "inputs/simple_test_3.txt"
#     map1 = map(FILE_PATH_1, readFile(FILE_PATH_1))
#     map2 = map(FILE_PATH_2, readFile(FILE_PATH_2))
#     map3 = map(FILE_PATH_3, readFile(FILE_PATH_3))
#     reduce1 = reduce([map1, map2, map3])
#     gt1 = {'quick': 1, 'brown': 1, 'fox': 1, 'jumps': 1, 'over': 1, 'the': 2, 'lazy': 1, 'dog': 1}
#     gt2 = {'the': 2, 'quick': 2, 'fox': 2, 'is': 2, 'orange': 2}
#     gt3 = {'fox': 1, 'jumps': 1, 'high': 1, 'over': 1, 'the': 2, 'lazy': 1, 'dog': 1}
#     gt  = {'quick': 3, 'brown': 1, 'fox': 4, 'jumps': 2, 'over': 2, 'the': 6, 'lazy': 2, 'dog': 2, 'is': 2, 'orange': 2, 'high': 1}
#     assert map1 == gt1, f"Expected {gt1}, got {map1}"
#     assert map2 == gt2, f"Expected {gt2}, got {map2}"
#     assert map3 == gt3, f"Expected {gt3}, got {map3}"
#     assert reduce1 == gt, f"Expected {gt}, got {reduce1}"
#     print(f"Final reduce: {reduce1}")
