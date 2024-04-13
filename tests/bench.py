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
    ## companies_sample = df['name'].sample(10_000)
    companies_sample = df['text'].sample(10_000)

    ## df.rename(columns={'name': 'contents', 'domain': 'id'}, inplace=True)
    df.rename(columns={'text': 'contents'}, inplace=True)
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

    


if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    ## FILENAME = os.path.join(CURRENT_DIR, '../../search-benchmark-game', 'og_corpus.json')
    ## FILENAME = os.path.join(CURRENT_DIR, '../../search-benchmark-game', 'corpus.json')
    ## FILENAME = os.path.join(CURRENT_DIR, '../../SearchApp/data', 'corpus.csv')
    ## FILENAME = os.path.join(CURRENT_DIR, '../../SearchApp/data', 'companies_sorted_100M.csv')
    FILENAME = os.path.join(CURRENT_DIR, '../../SearchApp/data', 'companies_sorted.csv')
    ## FILENAME = os.path.join(CURRENT_DIR, '../../SearchApp/data', 'companies_sorted_1M.csv')

    test_okapi_bm25(FILENAME)
    ## test_retriv(FILENAME)
    ## test_duckdb(FILENAME)
    ## test_anserini(FILENAME)
    ## test_sklearn(FILENAME)

    ## values = pd.read_csv(FILENAME, usecols=['name']).reset_index(drop=True)

    SEARCH_COL = 'name'
    ## SEARCH_COL = 'text'

    init = perf_counter()
    model = BM25(
            max_df=10000
            )
    model.index_file(filename=FILENAME, text_col=SEARCH_COL)
    ## model.index_documents(values['name'].tolist())
    model.save(db_dir='tmp_db')
    ## model.load(db_dir='tmp_db')
    print(f"Time to index: {perf_counter() - init:.2f} seconds")
    ## exit()

    N = 10_000
    ## names = pd.read_csv(FILENAME, usecols=[SEARCH_COL], nrows=N).reset_index(drop=True).name.tolist()
    names = pd.read_csv(
            ## names = pd.read_json(
            FILENAME, 
            usecols=[SEARCH_COL],
            nrows=N,
            ## lines=True
            ).reset_index(drop=True).fillna('')[SEARCH_COL].tolist()
    final_names = []
    for name in names:
        name = name.strip()
        if len(name) > 0:
            final_names.append(' '.join(np.random.choice(name.split(), 2)))
        else:
            final_names.append(name)

    init = perf_counter()
    for idx, name in enumerate(tqdm(final_names, desc="Querying")):
        model.get_topk_docs(name, init_max_df=10000)
        ## model.get_topk_indices(name, init_max_df=10000)

    time = perf_counter() - init
    print(f"Queries per second: {N / time:.2f}")

    QUERY = "netflix inc"

    records = model.get_topk_docs(QUERY, k=10, init_max_df=10000)
    '''
    scores, indices = model.get_topk_indices(QUERY, k=10, init_max_df=500)
    df = pd.DataFrame({
        'name': values['name'].iloc[indices].values,
        'scores': scores,
        'indices': indices
        })
    '''
    df = pd.DataFrame(records)
    print(df)

    ## scores, indices = model.get_topk_docs(query, k=10, init_max_df=500)
    ## print(names.iloc[indices])

