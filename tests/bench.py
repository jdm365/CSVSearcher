from rapid_bm25 import BM25
import pandas as pd
import polars as pl
import numpy as np
import os
from tqdm import tqdm
from time import perf_counter
from typing import List


def test_okapi_bm25(csv_filename: str, search_cols: List[str]):
    from rank_bm25 import BM25Okapi

    df = pl.read_csv(csv_filename)
    names = df.select(search_cols)

    companies_sample = names.select(search_cols).sample(1000).to_pandas()[search_cols]

    init = perf_counter()
    ## tokenized_names = [name.split() for name in tqdm(names, desc="Tokenizing")]
    tokenized_names = names.with_columns(
        pl.col(search_cols).fill_null('').str.split(' ').alias('tokens')
    ).select("tokens").to_series(0).to_list()
    bm25 = BM25Okapi(tokenized_names)
    print(f"Time to tokenize: {perf_counter() - init:.2f} seconds")

    for company in tqdm(companies_sample[:10], desc="Querying"):
        tok_company = company.split()
        bm25.get_top_n(tok_company, tokenized_names, n=10)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")


def test_retriv(csv_filename: str, search_cols: List[str]):
    from retriv import SparseRetriever

    df = pd.read_csv(csv_filename)
    names = df[search_cols]

    ## rename index id
    df['id'] = df.index.astype(str)

    rand_idxs = np.random.choice(len(names), 1000, replace=False)
    companies_sample = names.iloc[rand_idxs]

    init = perf_counter()
    model = SparseRetriever()
    model.index_file(
        path=csv_filename,
        show_progress=True,
        callback=lambda doc: {
            "id": doc['id'],
            "text": doc[search_cols]
        }
    )
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    for company in tqdm(companies_sample, desc="Querying"):
        company = str(company)
        model.search(
            query=company, 
            return_docs=True,
            cutoff=100
            )


def test_duckdb(csv_filename: str):
    import duckdb

    CONN = duckdb.connect(database=':memory:')
    CONN.execute(f"""
        CREATE TABLE companies AS (SELECT * FROM read_csv_auto('{csv_filename}'))
    """)

    init = perf_counter()
    CONN.execute("""
                 PRAGMA
                 create_fts_index('companies', 'domain', 'name')
                 """)
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    companies_sample = pd.read_csv(csv_filename)['name'].sample(1000)

    init = perf_counter()
    for company in tqdm(companies_sample, desc="Querying"):
        CONN.execute(f"""
                     SELECT name, domain, score FROM (
                         SELECT *, fts_main_documents.match_bm25(
                             domain,
                             "{company}"
                             ) AS score
                         FROM companies
                         LIMIT 10
                        ) sq
                     WHERE score IS NOT NULL
                     ORDER BY score DESC
                     """)
    print(f"Time to query: {perf_counter() - init:.2f} seconds")


def test_anserini(csv_filename: str, search_cols: List[str]):
    from pyserini.search import LuceneSearcher
    from pyserini.index.lucene import LuceneIndexer

    ## convert to json
    df = pl.read_csv(csv_filename)
    df = df.fill_null('')
    companies_sample = df.select(search_cols).sample(1000)

    df = df.with_columns(pl.lit(np.arange(len(df))).cast(str).alias('id'))

    df = df.with_columns(pl.col(search_cols).alias('contents'))
    os.system('rm -rf tmp_data_dir')
    os.system('mkdir tmp_data_dir')

    records = df.to_dicts()

    init = perf_counter()
    writer = LuceneIndexer('tmp_data_dir', append=True, threads=1)
    writer.add_batch_dict(records)
    writer.close()
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    searcher = LuceneSearcher('tmp_data_dir')
    lens = []
    for company in tqdm(companies_sample, desc="Querying"):
        hits = searcher.search(company, k=10)
        lens.append(len(hits))

    print(f"Time to query: {perf_counter() - init:.2f} seconds")
    print(f"Average number of hits: {np.mean(lens)}")
    print(f"Median number of hits:  {np.median(lens)}")

    os.system('rm -rf tmp_data_dir')

    del searcher, writer


def test_sklearn(csv_filename: str):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    df = pd.read_csv(csv_filename)
    names = df['name']

    rand_idxs = np.random.choice(len(names), 1000, replace=False)
    companies_sample = names.iloc[rand_idxs]

    init = perf_counter()
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(names)
    print(f"Time to vectorize: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    for company in tqdm(companies_sample[:10], desc="Querying"):
        company_vec = vectorizer.transform([company])
        cosine_similarity(X, company_vec)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")


def test_bm25_json(json_filename: str, search_cols: List[str]):
    df = pd.read_json(json_filename, lines=True, nrows=1000)
    sample = df[search_cols[0]].values

    init = perf_counter()
    model = BM25(stopwords='english')
    model.index_file(filename=json_filename, search_cols=search_cols)
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    for query in tqdm(sample, desc="Querying"):
        model.get_topk_docs(query, k=10)
    time = perf_counter() - init

    print(f"Queries per second: {1000 / time:.2f}")
    

def test_bm25_csv(csv_filename: str, search_cols: List[str]):
    df = pd.read_csv(csv_filename, usecols=search_cols, nrows=1000)
    sample = df[search_cols[0]].fillna('').astype(str).values

    init = perf_counter()
    model = BM25(max_df=25_000, stopwords='english')
    ## model = BM25(stopwords='english')
    model.index_file(filename=csv_filename, search_cols=search_cols)
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    ## Save and load
    init = perf_counter()
    model.save(db_dir='bm25_model')
    print(f"Time to save: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    ## model.load(db_dir='bm25_model')
    print(f"Time to load: {perf_counter() - init:.2f} seconds")

    lens = []
    init = perf_counter()
    for query in tqdm(sample, desc="Querying"):
        results = model.get_topk_docs(query, k=100)
        lens.append(len(results))
    time = perf_counter() - init

    print(f"Average number of hits: {np.mean(lens)}")
    print(f"Median number of hits:  {np.median(lens)}")

    print(f"Queries per second: {1000 / time:.2f}")

def test_bm25_parquet(filename: str, search_cols: List[str]):
    df = pd.read_parquet(filename, columns=search_cols).iloc[:1000]
    sample = df[search_cols].fillna('').astype(str).values

    init = perf_counter()
    model = BM25(max_df=50_000, stopwords=['netflix'])
    model.index_file(filename=filename, search_cols=search_cols)
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    for query in tqdm(sample, desc="Querying"):
        result = model.get_topk_docs(query, k=10)
    time = perf_counter() - init

    print(f"Queries per second: {1000 / time:.2f}")

def test_documents(csv_filename: str, search_cols: List[str]):
    df = pl.read_csv(csv_filename)
    names = df.select(search_cols).to_series(0)

    companies_sample = names.sample(1000)

    bm25 = BM25(min_df=10, max_df=50_000)

    init = perf_counter()
    bm25.index_documents(documents=names.to_list())
    print(f"Time to tokenize: {perf_counter() - init:.2f} seconds")

    for company in tqdm(companies_sample, desc="Querying"):
        bm25.get_topk_indices(company, k=10)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")

    ## Try saving and loading
    bm25.save(db_dir='bm25_model')
    bm25.load(db_dir='bm25_model')

    for company in tqdm(companies_sample[:10], desc="Querying"):
        bm25.get_topk_indices(company, k=10)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")

    os.system('rm -rf bm25_model')



if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

    CSV_FILENAME = os.path.join(CURRENT_DIR, 'mb.csv')
    JSON_FILENAME = os.path.join(CURRENT_DIR, 'mb.json')

    ## test_okapi_bm25(CSV_FILENAME, search_cols='title')
    ## test_retriv(CSV_FILENAME, search_cols='title')
    ## test_duckdb(FILENAME)
    ## test_anserini(CSV_FILENAME, search_cols='title')
    ## test_sklearn(FILENAME)
    ## test_bm25_csv(CSV_FILENAME, search_cols='text')
    ## test_bm25_csv(CSV_FILENAME, search_cols='name')
    test_bm25_csv(CSV_FILENAME, search_cols=['title', 'artist'])
    ## test_bm25_json(JSON_FILENAME, search_cols=['title', 'artist'])
    ## test_bm25_parquet(PARQUET_FILENAME, search_cols='name')
    ## test_documents(CSV_FILENAME, search_cols='title')
