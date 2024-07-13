from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import csv

from time import perf_counter
import os

from bloom25 import BM25

app = Flask(__name__)
CORS(app)


@app.route('/search', methods=['GET'])
def search():
    query_name   = request.args.get('name', '')
    query_artist = request.args.get('artist', '')
    init = perf_counter()
    results = search_app.get_search_results([query_name, query_artist])

    time_taken_ms = int(1e3) * (perf_counter() - init)
    return jsonify({'results': results, 'time_taken_ms': time_taken_ms})


@app.route('/columns', methods=['GET'])
def columns():
    column_names = search_app.get_column_names()
    ## Move search_col to the front
    for col in reversed(list(search_app.search_cols) + ['score']):
        column_names.remove(col)
        column_names.insert(0, col)
    return jsonify({'columns': column_names})


class SearchApp:
    def __init__(
            self,
            search_cols: str,
            filename: str
            ) -> None:
        self.filename = filename
        self.bm25 = BM25(
                ## stopwords='english',
                ## min_df=1,
                ## max_df=0.5,
                ## num_partitions=2,
                bloom_fpr=1e-8,
                ## b=0.4,
                ## k1=1.5
                )
        self.save_dir = filename.split('/')[-1].replace('.csv', '_db')
        try:
            raise Exception("Not loading BM25 index")
            self.bm25.load(db_dir=self.save_dir)
        except Exception as e:
            print(f"Error loading BM25 index: {e}")

            self.bm25.index_file(filename, search_cols)
            ## self.bm25.save(db_dir=self.save_dir)
            print(f"Saved BM25 index to {self.save_dir}")

        self.search_cols = search_cols

    def get_column_names(self):
        if self.filename.endswith('.csv'):
            with open(self.filename, newline='') as csvfile:
                reader = csv.reader(csvfile)
                columns = next(reader)
                columns.append('score')

        elif self.filename.endswith('.json'):
            ## Read first line
            with open(self.filename, 'r') as f:
                data = f.readlines()
                columns = list(json.loads(data[0]).keys())
                columns.append('score')
        else:
            raise ValueError(f"Unknown file type: {self.filename}\n Please provide a .csv or .json file.")

        cols = [x.lower() for x in columns if x.strip() != '']
        return cols

    def get_search_results(self, query: list) -> list:
        print(f"Query: {query}")

        init = perf_counter()
        vals = self.bm25.get_topk_docs(
                query, 
                k=100,
                ## boost_factors=[1, 1]
               )
        print(f"Query took {perf_counter() - init:.4f} seconds")

        return vals


if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    DATA_DIR = f"{CURRENT_DIR}/../tests"
    ## FILEPATH = os.path.join(DATA_DIR, 'mb.csv')
    ## SEARCH_COLS = ['title', 'artist']

    FILEPATH = os.path.join(DATA_DIR, 'wiki_articles.csv')
    SEARCH_COLS = ['title', 'body']

    search_app = SearchApp(
            filename=FILEPATH,
            search_cols=SEARCH_COLS
            )

    os.system(f"open {CURRENT_DIR}/index.html")

    app.run()
