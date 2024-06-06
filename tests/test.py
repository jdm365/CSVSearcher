from rank_bm25 import BM25Okapi
from rapid_bm25 import BM25
import pandas as pd
import numpy as np
from tqdm import tqdm
import os

pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


def test_csv_constructor(csv_filename: str, search_col: str = 'name'):
    df = pd.read_csv(csv_filename, usecols=[search_col])
    names = df[search_col].reset_index(drop=True)

    ## Strip all non-alphanumeric characters
    names = names.str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
    ## Remove extra whitespace
    names = names.str.replace(r'\s+', ' ', regex=True)

    rand_idxs = np.random.choice(len(names), 1000, replace=False)
    companies_sample = names.iloc[rand_idxs]

    tokenized_names = [name.split() for name in tqdm(names, desc="Tokenizing")]
    rank_bm25_model = BM25Okapi(tokenized_names)

    bm25_model = BM25()
    bm25_model.index_documents(documents=names.tolist())

    for company in tqdm(companies_sample, desc="Querying"):
        tok_company = company.split()
        scores = rank_bm25_model.get_scores(tok_company)
        results_rank_bm25 = np.argsort(scores)[::-1]
        results_rank_bm25 = results_rank_bm25[scores[results_rank_bm25] > 0]
        scores_rank_bm25 = scores[results_rank_bm25]

        scores, results_bm25 = bm25_model.get_topk_indices(company, k=1000000, query_max_df=100000)

        df_rank = pd.DataFrame({
            'rank_bm25': names.iloc[results_rank_bm25],
            'indices_rank_bm25': results_rank_bm25
        })
        df_rank['score'] = scores_rank_bm25
        df_rank['query'] = len(df_rank) * [tok_company]
        df = pd.DataFrame({
            'bm25': names.iloc[results_bm25],
            'indices_bm25': results_bm25
        })
        df['score'] = scores
        df['query'] = company
        ## print(df)
        ## print(df_rank)
        ## assert len(df) == len(df_rank)
        if len(df) != len(df_rank):
            if len(df) > len(df_rank):
                print(df[~df['bm25'].isin(df_rank['rank_bm25'])])
            else:
                print(df_rank[~df_rank['rank_bm25'].isin(df['bm25'])])
            print(f"len(df)={len(df)} != len(df_rank)={len(df_rank)}")
            break


if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    FILENAME = os.path.join(CURRENT_DIR, '../../SearchApp/data', 'companies_sorted_100k.csv')

    test_csv_constructor(FILENAME)
