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

    for company in tqdm(companies_sample[:10], desc="Querying"):
        tok_company = company.split()
        bm25.get_top_n(tok_company, tokenized_names, n=10)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")


def test_retriv(csv_filename: str):
    from retriv import SparseRetriever

    df = pd.read_csv(csv_filename)
    names = df['name']

    rand_idxs = np.random.choice(len(names), 10_000, replace=False)
    companies_sample = names.iloc[rand_idxs]

    init = perf_counter()
    model = SparseRetriever()
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

    companies_sample = pd.read_csv(csv_filename)['name'].sample(10_000)

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


def test_anserini(csv_filename: str):
    from pyserini.search import LuceneSearcher
    from pyserini.index.lucene import LuceneIndexer

    ## convert to json
    df = pd.read_csv(csv_filename).fillna('')
    companies_sample = df['name'].sample(10_000)
    ## companies_sample = df['text'].sample(10_000)

    df.rename(columns={'name': 'contents', 'domain': 'id'}, inplace=True)
    ## df.rename(columns={'text': 'contents'}, inplace=True)
    df['id'] = df.index.astype(str)
    os.system('rm -rf tmp_data_dir')
    os.system('mkdir tmp_data_dir')

    records = df.to_dict(orient='records')

    init = perf_counter()
    writer = LuceneIndexer('tmp_data_dir', append=True, threads=1)
    writer.add_batch_dict(records)
    writer.close()
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    searcher = LuceneSearcher('tmp_data_dir')
    for company in tqdm(companies_sample, desc="Querying"):
        hits = searcher.search(company, k=10)
        ## print(hits)
    print(f"Time to query: {perf_counter() - init:.2f} seconds")

    os.system('rm -rf tmp_data_dir')

    del searcher, writer


def test_sklearn(csv_filename: str):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    df = pd.read_csv(csv_filename)
    names = df['name']

    rand_idxs = np.random.choice(len(names), 10_000, replace=False)
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


def test_bm25_json(json_filename: str, search_col: str):
    df = pd.read_json(json_filename, lines=True, nrows=10_000)
    sample = df[search_col].values

    init = perf_counter()
    model = BM25(max_df=10_000)
    model.index_file(filename=json_filename, text_col=search_col)
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    for query in tqdm(sample, desc="Querying"):
        model.get_topk_docs(query, k=10)
    time = perf_counter() - init

    print(f"Queries per second: {10_000 / time:.2f}")
    

def test_bm25_csv(csv_filename: str, search_col: str):
    df = pd.read_csv(csv_filename, usecols=[search_col], nrows=10_000)
    sample = df[search_col].fillna('').astype(str).values

    init = perf_counter()
    model = BM25(max_df=50_000)
    model.index_file(filename=csv_filename, text_col=search_col)
    print(f"Time to index: {perf_counter() - init:.2f} seconds")

    init = perf_counter()
    for query in tqdm(sample, desc="Querying"):
        model.get_topk_docs(query, k=10)
    time = perf_counter() - init

    print(f"Queries per second: {10_000 / time:.2f}")

def test_documents(csv_filename: str, search_col: str):
    df = pd.read_csv(csv_filename)
    names = df[search_col].tolist()

    rand_idxs = np.random.choice(len(names), 10_000, replace=False)
    companies_sample = [names[i] for i in rand_idxs]

    bm25 = BM25(min_df=10, max_df=50_000)

    init = perf_counter()
    bm25.index_documents(documents=names)
    print(f"Time to tokenize: {perf_counter() - init:.2f} seconds")

    for company in tqdm(companies_sample, desc="Querying"):
        bm25.get_topk_indices(company, k=10)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")

    ## Try saving and loading
    bm25.save(db_dir='bm25_model')
    bm25.load(db_dir='bm25_model')

    ## Go again.

    init = perf_counter()
    bm25.index_documents(documents=names)
    print(f"Time to tokenize: {perf_counter() - init:.2f} seconds")

    for company in tqdm(companies_sample[:10], desc="Querying"):
        bm25.get_topk_indices(company, k=10)

    print(f"Time to query: {perf_counter() - init:.2f} seconds")

    os.system('rm -rf bm25_model')



if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

    JSON_FILENAME = os.path.join(CURRENT_DIR, '../../search-benchmark-game', 'og_corpus.json')
    ## JSON_FILENAME = os.path.join(CURRENT_DIR, '../../search-benchmark-game', 'corpus.json')

    ## CSV_FILENAME = os.path.join(CURRENT_DIR, '../../SearchApp/data', 'corpus.csv')
    CSV_FILENAME = os.path.join(CURRENT_DIR, '../../SearchApp/data', 'companies_sorted.csv')

    ## test_okapi_bm25(FILENAME)
    ## test_retriv(CSV_FILENAME)
    ## test_duckdb(FILENAME)
    ## test_anserini(CSV_FILENAME)
    ## test_sklearn(FILENAME)
    ## test_bm25_csv(CSV_FILENAME, search_col='text')
    test_bm25_json(JSON_FILENAME, search_col='text')
    test_bm25_csv(CSV_FILENAME, search_col='name')
    test_documents(CSV_FILENAME, search_col='name')
