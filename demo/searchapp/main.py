from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import csv

from time import perf_counter
import os

from bloom25 import BM25

app = Flask(__name__)
CORS(app)

@app.route('/healthcheck', methods=['HEAD'])
def healthcheck():
    return '', 200

@app.route('/search', methods=['GET'])
def search():
    query = {}
    for col in search_app.search_cols:
        query[col] = request.args.get(col, '')

    init = perf_counter()
    results = search_app.get_search_results(query)
    time_taken_ms = int(1e3) * (perf_counter() - init)

    return jsonify({'results': results, 'time_taken_ms': time_taken_ms})


@app.route('/get_columns', methods=['GET'])
def columns():
    column_names = search_app.get_column_names()
    ## Move search_col to the front
    for col in reversed(list(search_app.search_cols) + ['score']):
        column_names.remove(col)
        column_names.insert(0, col)
    return jsonify({'columns': column_names})

@app.route('/get_search_columns', methods=['GET'])
def search_columns():
    column_names = search_app.search_cols
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
                ## num_partitions=1,
                ## bloom_df_threshold=1.0,
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

    def get_search_results(self, query: dict) -> list:
        ## if len(query) == 0:
            ## return []
        print(f"Query: {query}")

        init = perf_counter()
        vals = self.bm25.get_topk_docs(
                query,
                k=100,
                boost_factors=[500, 2, 1, 1]
               )
        print(f"Query took {perf_counter() - init:.4f} seconds")

        return vals


if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    ## DATA_DIR = f"{CURRENT_DIR}/../"
    DATA_DIR = f"{CURRENT_DIR}/"

    ## FILEPATH = os.path.join(DATA_DIR, 'nodes_export_subset.csv')
    ## SEARCH_COLS = ['er_name', 'er_address']

    ## FILEPATH = os.path.join(DATA_DIR, 'tests/free_company_dataset.csv')
    ## SEARCH_COLS = ['name', 'location']
    FILEPATH = os.path.join(DATA_DIR, 'planet.csv')
    SEARCH_COLS = ['street', 'city', 'housenumber', 'iso3']

    search_app = SearchApp(
            filename=FILEPATH,
            search_cols=SEARCH_COLS
            )

    os.system(f"open {CURRENT_DIR}/index.html")

    app.run()
