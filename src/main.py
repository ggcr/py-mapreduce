import os
import sys
import glob
import argparse

from src import utils
from src.driver import Driver

def sequential_count(FILES: list[str]):
    res = {}
    for i in range(0, len(FILES)):
        cont = utils.readFile(FILES[i])
        for word in cont.split(' '):
            res[word] = res.get(word, 0) + 1
    return res

if __name__ == '__main__':
    """
    Usage: python3 -m src.main -N 3 -M 4 inputs/*
    """
    parser = argparse.ArgumentParser(description="MapReduce microframework through HTTP.")
    parser.add_argument('-N', type=int, default=3, required=True, help="Number of map tasks")
    parser.add_argument('-M', type=int, default=4, required=True, help="Number of reduce tasks")
    parser.add_argument('FILES', nargs='+', default=['inputs/*'], help="List of input file paths")
    args = parser.parse_args()

    N = args.N
    M = args.M
    FILES = [] # to support wildcards e.g. `inputs/*`
    for file_pattern in args.FILES:
        FILES.extend(glob.glob(file_pattern))

    driver = Driver(N, M)
    res = driver.run(FILES)
    # print(res)
    GT = sequential_count(FILES)
    print(f"[MAIN] Testing the accumulated output with the sequential count.")
    assert res == GT, f"Expected {GT}, got {res}"
    print(f"[MAIN] ✅ Result is correct.")
