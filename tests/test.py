from bm25 import BM25
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from time import perf_counter


def test_okapi_bm25(csv_filename: str):
    from rank_bm25 import BM25Okapi

    df = pd.read_csv(csv_filename)
    names = df['name']

    rand_idxs = np.random.choice(len(names), 10_000, replace=False)
    companies_sample = names.iloc[rand_idxs]

    init = perf_counter()
    tokenized_names = [name.split() for name in tqdm(names, desc="Tokenizing")]
    bm25 = BM25Okapi(tokenized_names)
    print(f"Time to tokenize: {perf_counter() - init:.2f} seconds")

    for company in tqdm(companies_sample, desc="Querying"):
        tok_company = company.split()
        bm25.get_top_n(tok_company, tokenized_names, n=10)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")


def test_retriv(csv_filename: str):
    from retriv import SparseRetriever

    df = pd.read_csv(csv_filename)
    names = df['name']

    rand_idxs = np.random.choice(len(names), 10_000, replace=False)
    companies_sample = names.iloc[rand_idxs]
    companies_sample_dict = df.iloc[rand_idxs].rename(
        columns={'name': 'text', 'domain': 'id'}
    ).to_dict(orient='records')

    init = perf_counter()
    model = SparseRetriever()
    '''
    sr = sr.index_file(
      path="path/to/collection",  # File kind is automatically inferred
      show_progress=True,         # Default value
      callback=lambda doc: {      # Callback defaults to None.
        "id": doc["id"],
        "text": doc["title"] + ". " + doc["text"],          
      )
      '''
    model.index_file(
        path=csv_filename,
        show_progress=True,
        callback=lambda doc: {
            "id": doc["domain"],
            "text": doc["name"]
        }
    )
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    for company in tqdm(companies_sample, desc="Querying"):
        model.search(
            query=company, 
            return_docs=True,
            cutoff=10
            )


if __name__ == '__main__':
    query = "netflix inc"

    FILENAME = '/home/jdm365/SearchApp/data/companies_sorted.csv'
    ## FILENAME = '/Users/jakemehlman/Kaggle/Kaggle_Competition_Foursquare/data/train.csv'
    ## FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted_1M.csv'


    ## FILENAME = '/home/jdm365/search-benchmark-game/corpus_500k.json'
    ## FILENAME = '/home/jdm365/search-benchmark-game/corpus.json'
    ## print(os.system(f"head -5 {FILENAME}"))

    ## test_okapi_bm25(FILENAME)
    ## test_retriv(FILENAME)

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
            ## text_col='text',
            text_col='name',
            ## db_dir='bm25_db'
            max_df=(10000/7.2e6)
            )
    os.system(f"rm -rf bm25_db")
    ## exit()
    names = pd.read_csv(FILENAME, usecols=['name']).reset_index(drop=True).name.tolist()

    rand_idxs = np.random.choice(len(names), 10_000, replace=False)


    init = perf_counter()
    for idx in tqdm(rand_idxs, desc="Querying"):
        model.query(names[idx], init_max_df=500)
        ## records = model.get_topk_docs(names[idx], k=10, init_max_df=500)
    time = perf_counter() - init
    print(f"Time taken: {time:.2f} seconds")
    print(f"Queries per second: {len(rand_idxs) / time:.2f}")

    ##scores, indices = model.query(query)
    ## time_us = (perf_counter() - init) * 1e6
    ## print(f"Time to get top 10: {time_us:.2f} micro seconds")

    ## time_us = (perf_counter() - init) * 1e6
    ## print(f"Time to get top 10: {time_us:.2f} micro seconds")
    records = model.get_topk_docs(query, k=10, init_max_df=500)
    print(pd.DataFrame(records))
    ## print(scores)
    ## print(indices)
