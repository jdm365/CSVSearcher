import polars as pl
from tqdm import tqdm
from pypruningradixtrie.trie import PruningRadixTrie
from pypruningradixtrie.insert import insert_term
from time import perf_counter_ns

def perf_counter():
    return perf_counter_ns() * 0.001



if __name__ == "__main__":
    FILENAME = 'data/terms.txt'
    df = pl.read_csv(FILENAME, separator='\t', has_header=False)
    df.columns = ['term', 'score']

    df = df.with_columns(
            pl.col('score').fill_null(0)
        )
    
    trie = PruningRadixTrie()

    for row in tqdm(df.iter_rows(named=True), total=len(df)):
        insert_term(trie, term=row['term'], term_score=row['score'])

    term = 'microsoft'
    for i in range(1, len(term) + 1):
        init = perf_counter()
        trie.get_top_k_for_prefix(term[:i], 10)
        end = perf_counter()
        print(f"Time to get top 10 terms starting with '{term[:i]}': {end - init}us")


    init = perf_counter()
    print(trie.get_top_k_for_prefix("m", 10))
    end = perf_counter()
    print(f"Time to get top 10 terms starting with 'm': {end - init}us")
