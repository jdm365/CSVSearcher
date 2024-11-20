from radixtrie import RadixTrie
from tqdm import tqdm
import random
import string
from time import perf_counter


def test_insert(N: int = 1_000_000, avg_len: int = 6):

    strings = []
    for i in range(N):
        strings.append("")
        for _ in range(avg_len):
            strings[i] += random.choice(string.ascii_letters)

    init = perf_counter()
    trie = RadixTrie()
    for i in tqdm(range(N)):
        trie.insert(strings[i], i)

    print(f"Trie Insertion of {N} strings of average length {avg_len} took {perf_counter() - init:.2f} seconds")
    print(f"Trie Insertions per second: {N / (perf_counter() - init):.2f}")


    dict_test = {}
    init = perf_counter()
    for i in tqdm(range(N)):
        dict_test[strings[i]] = i

    print(f"Dict Insertion of {N} strings of average length {avg_len} took {perf_counter() - init:.2f} seconds")
    print(f"Dict Insertions per second: {N / (perf_counter() - init):.2f}")

    return trie, strings

def test_find(trie, strings: list):
    init = perf_counter()
    for i in tqdm(range(len(strings))):
        trie.find(strings[i])

    print(f"Trie Finding {len(strings)} strings took {perf_counter() - init:.2f} seconds")
    print(f"Trie Lookups per second: {len(strings) / (perf_counter() - init):.2f}")

    dict_test = {}
    for i in range(len(strings)):
        dict_test[strings[i]] = i

    init = perf_counter()
    for i in tqdm(range(len(strings))):
        dict_test[strings[i]]

    print(f"Dict Finding {len(strings)} strings took {perf_counter() - init:.2f} seconds")
    print(f"Dict Lookups per second: {len(strings) / (perf_counter() - init):.2f}")


if __name__ == '__main__':
    trie = RadixTrie()
    trie.insert('hello', 'world')
    trie.insert('world', 'hello')
    trie.insert('hi', 42)
    trie.insert('hey', 43)

    print(trie.find('hello'))  # Output: world
    print(trie.find('world'))  # Output: hello
    print(trie.find('hi'))  # Output: 42
    print(trie.find('hey'))  # Output: 43
    print(trie.find('hellope'))  # Output: world

    trie, strings = test_insert()
    test_find(trie, strings)
