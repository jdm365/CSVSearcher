from bm25 import BM25
import pandas as pd
from time import perf_counter



if __name__ == '__main__':
    query = "netflix"

    FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted.csv'
    ## names = pd.read_csv(FILENAME)['name'].str.lower().tolist()
    '''
    names_df = pd.read_csv(
            FILENAME,
            usecols=['name'],
            nrows=100_000
        )
    names_df['name'] = names_df['name'].str.lower()

    init = perf_counter()
    model = BM25(documents=names_df['name'].tolist())
    print(f"Time to build model: {perf_counter() - init:.2f} seconds")
    print(f"Thousand documents per second: {int(len(names_df) * 0.001 / (perf_counter() - init))}")

    print(f"IDF: {model._compute_idf('netflix')}")



    init = perf_counter()
    scores, topk = model.query(query, k=10)
    print(f"Time to get top 10: {perf_counter() - init:.5f} seconds")

    print(f"Scores: {scores}")
    print(f"Topk:   {topk}")

    print(names_df.iloc[topk])
    '''

    model = BM25(
            csv_file=FILENAME, 
            text_column='name', 
            whitespace_tokenization=True, 
            ngram_size=6
            )

    init = perf_counter()
    records = model.get_topk_docs(query, k=10)
    print(f"Time to get top 10: {perf_counter() - init:.5f} seconds")

    print(pd.DataFrame(records))
