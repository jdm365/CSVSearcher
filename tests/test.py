from bm25 import BM25
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from time import perf_counter



if __name__ == '__main__':
    query = "netflix inc"

    FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted.csv'
    ## FILENAME = '/Users/jakemehlman/Kaggle/Kaggle_Competition_Foursquare/data/train.csv'
    ## FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted_1M.csv'

    names = pd.read_csv(FILENAME, usecols=['name']).reset_index(drop=True).name.tolist()

    ## FILENAME = '/home/jdm365/search-benchmark-game/corpus_500k.json'
    FILENAME = '/home/jdm365/search-benchmark-game/corpus.json'
    print(os.system(f"head -5 {FILENAME}"))
    init = perf_counter()
    ## df = pd.read_json(FILENAME, lines=True)
    time = perf_counter() - init
    print(f"Time taken: {time:.2f} seconds")
    ## exit()
    ## print(df)
    ## df.to_json('/home/jdm365/search-benchmark-game/corpus_500k.json', orient='records', lines=True)
    ## FILENAME = '/home/jdm365/search-benchmark-game/corpus.json'

    model = BM25(
            filename=FILENAME, 
            ## text_col='name',
            text_col='text',
            cache_doc_term_freqs=True,
            cache_inverted_index=True,
            cache_term_freqs=True,
            )

    rand_idxs = np.random.choice(len(names), 10_000, replace=False)

    scores, indices = model.query(query)

    init = perf_counter()
    for idx in tqdm(rand_idxs, desc="Querying"):
        ## scores, indices = model.query(names[idx])
        records = model.get_topk_docs(names[idx], k=10)
    time = perf_counter() - init
    print(f"Time taken: {time:.2f} seconds")
    print(f"Queries per second: {len(rand_idxs) / time:.2f}")

    ##scores, indices = model.query(query)
    ## time_us = (perf_counter() - init) * 1e6
    ## print(f"Time to get top 10: {time_us:.2f} micro seconds")

    ## time_us = (perf_counter() - init) * 1e6
    ## print(f"Time to get top 10: {time_us:.2f} micro seconds")
    records = model.get_topk_docs(query, k=10, init_max_df=1000)
    print(pd.DataFrame(records))
    ## print(scores)
    ## print(indices)
