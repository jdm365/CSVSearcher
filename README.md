# rapid_bm25
BM25 algorithm written in c++ and exposed through cython.

## Install
```bash
git clone https://github.com/jdm365/BM25.git
cd BM25
pip install .
```

## Usage

### From File
```python
from rapid_bm25 import BM25

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
    min_df=1,
    max_df=0.0001,
    k1=K1,
    b=B
)
model.index_file(
    filename=filename,
    text_col=search_col
)

QUERY = 'hello world'
K = 50

## Only analyze documents containing token with fewer than
## this number of occurences. Smaller numbers speed up queries
## and have limited impact on result ordering. If no documents
## are found with the given init_max_df, it is automatically increased 
## until results are found.
INIT_MAX_DF = 5000

## Returns topk records with "score" property in json (dict) format.
top_k_records = model.get_topk_docs(
    query=QUERY,
    k=K,
    init_max_df=INIT_MAX_DF
)

## Or use raw query to just get scores and indices.
scores, indices = model.get_topk_indices(
    query=QUERY,
    k=K,
    init_max_df=INIT_MAX_DF
)

## Save and load
DB_DIR = 'bm25_db'
model.save(db_dir=DB_DIR)
model.load(db_dir=DB_DIR)
```

### From Documents
```python
from rapid_bm25 import BM25

## Documents being an arraylike of strings to search.
import pandas as pd

df = pd.read_csv('data.csv')
documents = df['text']

## Okapi BM25 params
K1 = 1.2
B = 0.75

## Documents constructor.
model = BM25(
    min_df=1,
    max_df=0.0001,
    k1=K1,
    b=B
)
model.index_documents(
    documents=documents
)

QUERY = 'hello world'
K = 50
INIT_MAX_DF = 5000

## NOTE: get_topk_docs is not available without a file to fetch the documents from
## therefore it is only supported with the file constructor.

scores, indices = model.get_topk_indices(
    query=QUERY,
    k=K,
    init_max_df=INIT_MAX_DF
)

## Save and load
DB_DIR = 'bm25_db'
model.save(db_dir=DB_DIR)
model.load(db_dir=DB_DIR)
```

In earlier stages at the moment and likely still buggy and potentially incorrect. 
Currently all text is converted to utf-8 and upper cased and only whitespace
tokenization is supported.

An inverted index structure and dynamic init_max_df are used for faster lookup much
like Lucene and other full-text search engines. It also removes some of the constant
factors from the Okapi-BM25 scoring which do not affect ordering of the results.
This code uses the Robin Hood library from c/c++ for hashing
which boosts performance further.

As best I can tell this search is on-par or slightly faster than other python library
equivalents (which use inverted indexees) and can index documents (especially shorter docs)
much faster than other libraries I've tested like rank_bm25, retriv, and anserini, though
this needs to be proved further.

As of now the only dependency is the c++ stdlib.
