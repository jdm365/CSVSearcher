# (In Development) - Currently Porting to Zig and making breaking changes.
## Much of what's below is outdated and will be updated soon.
FTS engine with a focus on fast indexing of csv files.

## Install
```bash
git clone https://github.com/jdm365/BM25.git
cd BM25
pip install .
```

## Usage

### From File
```python
from bloom25 import BM25

## Current supported file types are csv and json.
filename = 'data.csv'
search_col = 'text'

## Okapi BM25 params
K1 = 1.2
B = 0.75

## Raw file constructor.
## Pass in filename directly. Loaded in c++ backend and enables getting topk
## records with memory mapped files.
model = BM25(
    bloom_df_threshold=0.01,
    bloom_fpr=1e-8,
    k1=K1,
    b=B
)
model.index_file(
    filename=filename,
    text_col=search_col
)

QUERY = 'hello world'
K = 10

## Only analyze documents containing token with fewer than
## this number of occurences. Smaller numbers speed up queries
## and have limited impact on result ordering. If no documents
## are found with the given init_max_df, it is automatically increased 
## until results are found.
QUERY_MAX_DF = 5000

## Returns topk records with "score" property in json (dict) format.
top_k_records = model.get_topk_docs(
    query=QUERY,
    k=K,
    query_max_df=QUER_MAX_DF
)

## Or use raw query to just get scores and indices.
scores, indices = model.get_topk_indices(
    query=QUERY,
    k=K,
    query_max_df=QUERY_MAX_DF
)

## Save and load
DB_DIR = 'bm25_db'
model.save(db_dir=DB_DIR)
model.load(db_dir=DB_DIR)
```

### From Documents
```python
from bloom25 import BM25

## Documents being an arraylike of strings to search.
import pandas as pd

df = pd.read_csv('data.csv')
documents = df['text']

## Okapi BM25 params
K1 = 1.2
B = 0.75

## Documents constructor.
model = BM25(
    bloom_df_threshold=0.01,
    bloom_fpr=1e-8,
    k1=K1,
    b=B
)
model.index_documents(
    documents=documents
)

QUERY = 'hello world'
K = 50
QUERY_MAX_DF = 5000

## NOTE: get_topk_docs is not available without a file to fetch the documents from
## therefore it is only supported with the file constructor.

scores, indices = model.get_topk_indices(
    query=QUERY,
    k=K,
    query_max_df=QUERY_MAX_DF
)

## Save and load
DB_DIR = 'bm25_db'
model.save(db_dir=DB_DIR)
model.load(db_dir=DB_DIR)
```
