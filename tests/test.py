from bm25 import BM25
import pandas as pd
import numpy as np
from tqdm import tqdm
from time import perf_counter



if __name__ == '__main__':
    query = "netflix inc"

    ## FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted.csv'
    FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted_1M.csv'
    names = pd.read_csv(FILENAME, usecols=['name']).reset_index(drop=True).name.tolist()

    model = BM25(
            csv_file=FILENAME, 
            text_column='name',
            max_df=0.001
            )

    rand_idxs = np.random.choice(len(names), 1_000_00, replace=False)

    scores, indices = model.query(query)

    init = perf_counter()
    for idx in tqdm(rand_idxs, desc="Querying"):
        ## scores, indices = model.query(names[idx])
        records = model.get_topk_docs(names[idx], k=10)
        ## records = model._get_topk_internal(query, 10)
    time = perf_counter() - init
    print(f"Time taken: {time:.2f} seconds")
    print(f"Queries per second: {len(rand_idxs) / time:.2f}")

    ##scores, indices = model.query(query)
    ## time_us = (perf_counter() - init) * 1e6
    ## print(f"Time to get top 10: {time_us:.2f} micro seconds")

    ## time_us = (perf_counter() - init) * 1e6
    ## print(f"Time to get top 10: {time_us:.2f} micro seconds")
    records = model.get_topk_docs(query, k=10)
    print(pd.DataFrame(records))
    ## print(scores)
    ## print(indices)
