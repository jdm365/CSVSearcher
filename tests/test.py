from bm25 import BM25
import pandas as pd
from time import perf_counter, sleep



if __name__ == '__main__':
    query = "netflix inc"

    FILENAME = '/home/jdm365/SearchApp/basic_search/data/companies_sorted.csv'
    names = pd.read_csv(FILENAME, usecols=['name'])

    model = BM25(
            csv_file=FILENAME, 
            text_column='name', 
            ## documents=names['name'].tolist(),
            whitespace_tokenization=True, 
            ngram_size=6,
            )

    init = perf_counter()
    ## records = model.get_topk_docs(query, k=10)
    print(f"Time to get top 10: {perf_counter() - init:.5f} seconds")

    ## print(pd.DataFrame(records))

    init = perf_counter()
    ## records = model.get_topk_docs(query, k=10)
    print(f"Time to get top 10: {perf_counter() - init:.5f} seconds")

    sleep(10)
