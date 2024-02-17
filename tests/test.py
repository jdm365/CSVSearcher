from bm25 import BM25
import pandas as pd
from time import perf_counter



if __name__ == '__main__':
    query = "netflix inc"

    FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted.csv'

    model = BM25(
            csv_file=FILENAME, 
            text_column='name', 
            whitespace_tokenization=True, 
            ngram_size=6,
            )

    init = perf_counter()
    records = model.get_topk_docs(query, k=10)
    print(f"Time to get top 10: {perf_counter() - init:.5f} seconds")

    print(pd.DataFrame(records))
