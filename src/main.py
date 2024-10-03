import os
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
    FILES = ["inputs/simple_test_1.txt", "inputs/simple_test_2.txt", "inputs/simple_test_3.txt",]
    N = 20 # map tasks
    M = 4 # reduce tasks
    driver = Driver(N, M)
    res = driver.run(FILES)
    print(res)
    GT = sequential_count(FILES)
    assert res == GT, f"Expected {GT}, got {res}"

    FILES = ['inputs/pg-sherlock_holmes.txt', 'inputs/pg-tom_sawyer.txt', 'inputs/pg-frankenstein.txt', 'inputs/pg-grimm.txt', 'inputs/pg-being_ernest.txt', 'inputs/pg-huckleberry_finn.txt', 'inputs/pg-metamorphosis.txt', 'inputs/pg-dorian_gray.txt']
    N = 20 # map tasks
    M = 4 # reduce tasks
    driver = Driver(N, M)
    res = driver.run(FILES)
    # print(res)
    GT = sequential_count(FILES)
    assert res == GT, f"Expected {GT}, got {res}"
