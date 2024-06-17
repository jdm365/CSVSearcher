# cython: language_level=3

cimport cython

from libc.stdint cimport uint16_t, int32_t, uint32_t, uint64_t 
from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.pair cimport pair
from libcpp cimport bool

from time import perf_counter
import os


cdef vector[string] ENGLISH_STOPWORDS = {
	b"I", b"ME", b"MY", b"MYSELF", b"WE", b"OUR", b"OURS", b"OURSELVES", b"YOU", b"YOUR", b"YOURS",
	b"YOURSELF", b"YOURSELVES", b"HE", b"HIM", b"HIS", b"HIMSELF", b"SHE", b"HER", b"HERS", b"HERSELF", b"IT",
	b"ITS", b"ITSELF", b"THEY", b"THEM", b"THEIR", b"THEIRS", b"THEMSELVES", b"WHAT", b"WHICH", b"WHO", b"WHOM",
	b"THIS", b"THAT", b"THESE", b"THOSE", b"AM", b"IS", b"ARE", b"WAS", b"WERE", b"BE", b"BEEN",
	b"BEING", b"HAVE", b"HAS", b"HAD", b"HAVING", b"DO", b"DOES", b"DID", b"DOING", b"A", b"AN",
	b"THE", b"AND", b"BUT", b"IF", b"OR", b"BECAUSE", b"AS", b"UNTIL", b"WHILE", b"OF", b"AT",
	b"BY", b"FOR", b"WITH", b"ABOUT", b"AGAINST", b"BETWEEN", b"INTO", b"THROUGH", b"DURING", b"BEFORE", b"AFTER",
	b"ABOVE", b"BELOW", b"TO", b"FROM", b"UP", b"DOWN", b"IN", b"OUT", b"ON", b"OFF", b"OVER",
	b"UNDER", b"AGAIN", b"FURTHER", b"THEN", b"ONCE", b"HERE", b"THERE", b"WHEN", b"WHERE", b"WHY", b"HOW",
	b"ALL", b"ANY", b"BOTH", b"EACH", b"FEW", b"MORE", b"MOST", b"OTHER", b"SOME", b"SUCH", b"NO",
	b"NOR", b"NOT", b"ONLY", b"OWN", b"SAME", b"SO", b"THAN", b"TOO", b"VERY", b"S", b"T",
	b"CAN", b"WILL", b"JUST", b"DON", b"SHOULD", b"NOW" 
}

cdef int INT_MAX = 2147483647

cdef extern from "engine.h":
    ctypedef struct BM25Result:
        uint64_t doc_id 
        float score
        uint16_t partition_id

    cdef cppclass _BM25:
        _BM25(
                string filename,
                vector[string] search_col,
                int   min_df,
                float max_df,
                float k1,
                float b,
                uint16_t num_partitions,
                const vector[string]& stopwords
                ) nogil
        _BM25(string db_dir) nogil
        _BM25(
                vector[vector[string]]& documents,
                int   min_df,
                float max_df,
                float k1,
                float b,
                uint16_t num_partitions,
                const vector[string]& stopwords
                ) nogil
        vector[BM25Result] query(
                string& term, 
                uint32_t top_k, 
                uint32_t query_max_df,
                vector[float] boost_factors
                ) nogil
        vector[vector[pair[string, string]]] get_topk_internal(
                string& term, 
                uint32_t k, 
                uint32_t query_max_df,
                vector[float] boost_factors
                ) nogil 
        void save_to_disk(string db_dir) nogil
        void load_from_disk(string db_dir) nogil

        
def is_pandas_dataframe(obj):
    return type(obj).__name__ == 'DataFrame' and hasattr(obj, 'loc') and hasattr(obj, 'iloc')

def is_pandas_series(obj):
    return type(obj).__name__ == 'Series' and hasattr(obj, 'values') and hasattr(obj, 'index')

def is_polars_dataframe(obj):
    return type(obj).__name__ == 'DataFrame' and hasattr(obj, 'select') and hasattr(obj, 'filter')

def is_polars_series(obj):
    return type(obj).__name__ == 'Series' and hasattr(obj, 'to_frame') and hasattr(obj, 'name')

def is_numpy_array(obj):
    return type(obj).__name__ == 'ndarray'

cdef class BM25:
    cdef _BM25* bm25
    cdef int    min_df
    cdef float  max_df
    cdef str    filename 
    cdef str    db_dir
    cdef float  k1 
    cdef float  b
    cdef bool   is_parquet
    cdef vector[string] stopwords
    cdef uint16_t num_partitions
    cdef vector[string] search_cols


    def __init__(
            self, 
            int   min_df = 1,
            float max_df = 1.0,
            float k1     = 1.2,
            float b      = 0.4,
            stopwords = [],
            int   num_partitions = os.cpu_count()
            ):
        self.min_df = min_df
        self.max_df = max_df
        self.k1     = k1
        self.b      = b

        if num_partitions < 1:
            num_partitions = os.cpu_count()

        self.num_partitions = num_partitions

        if stopwords == 'english':
            self.stopwords = ENGLISH_STOPWORDS
        else:
            for stopword in stopwords:
                self.stopwords.push_back(stopword.upper().encode("utf-8"))


    def index_file(self, str filename, list search_cols):
        self.filename = filename
        for text_col in search_cols:
            self.search_cols.push_back(text_col.lower().encode("utf-8"))

        self._init_with_file(filename, self.search_cols)


    def index_documents(self, documents):
        assert len(documents) > 0, "Document count must be greater than 0"

        self.filename = "in_memory"

        if isinstance(documents[0], tuple) or isinstance(documents[0], list):
            self._init_lists(documents)
        elif isinstance(documents[0], dict):
            self._init_dicts(documents)
        elif isinstance(documents[0], str):
            self._init_documents(documents)
        elif is_pandas_dataframe(documents):
            documents.fillna('', inplace=True)
            self._init_lists(documents.values.tolist())
        elif is_pandas_series(documents):
            documents.fillna('', inplace=True)
            self._init_documents(documents.tolist())
        elif is_polars_dataframe(documents):
            self._init_lists(documents.rows())
        elif is_polars_series(documents):
            documents = documents.str.fill_null("")
            self._init_documents(documents.to_list())
        else:
            raise ValueError("Documents must be list, tuple, or dict.")


    def save(self, db_dir):
        self.db_dir = db_dir

        self.bm25.save_to_disk(db_dir.encode("utf-8"))

        ## Get from os when dir was last modified
        last_modified = os.path.getmtime(self.db_dir)

        ## Write to a file
        with open(os.path.join(self.db_dir, "last_modified.txt"), "w") as f:
            f.write(str(last_modified))

        ## Write to a file
        with open(os.path.join(self.db_dir, "filename.txt"), "w") as f:
            f.write(self.filename)


    def load(self, db_dir):
        self.db_dir = db_dir

        ## First check if db_dir exists
        if not os.path.exists(self.db_dir):
            raise RuntimeError("Database directory does not exist")

        with open(os.path.join(self.db_dir, "filename.txt"), "r") as f:
            self.filename = f.read()

        self.bm25 = new _BM25(self.db_dir.encode("utf-8"))
        return True


    cdef void _init_lists(self, list documents):
        init = perf_counter()

        cdef vector[vector[string]] docs
        docs.resize(len(documents))
        cdef str doc
        for idx, doc_list in enumerate(documents):
            for doc in doc_list:
                if doc is None:
                    docs[idx].push_back("".encode("utf-8"))
                    continue

                docs[idx].push_back(doc.upper().encode("utf-8"))

        self.bm25 = new _BM25(
                docs,
                self.min_df,
                self.max_df,
                self.k1,
                self.b,
                self.num_partitions,
                self.stopwords
                )

    cdef void _init_dicts(self, list documents):
        init = perf_counter()

        self.search_cols = sorted(self.search_cols)

        cdef vector[vector[string]] docs
        docs.resize(len(documents))
        cdef str doc
        for idx, doc_list in enumerate(documents):
            doc_list = dict(sorted(doc_list.items(), key=lambda x: x[0]))
            for doc in doc_list:
                docs[idx].push_back(doc.upper().encode("utf-8"))

        self.bm25 = new _BM25(
                docs,
                self.min_df,
                self.max_df,
                self.k1,
                self.b,
                self.num_partitions,
                self.stopwords
                )

    cdef void _init_documents(self, list documents):
        init = perf_counter()

        cdef vector[vector[string]] docs
        docs.resize(len(documents))
        cdef str doc
        for idx, doc in enumerate(documents):
            docs[idx].push_back(doc.upper().encode("utf-8"))

        self.bm25 = new _BM25(
                docs,
                self.min_df,
                self.max_df,
                self.k1,
                self.b,
                self.num_partitions,
                self.stopwords
                )

    cdef void _init_with_file(self, str filename, vector[string] search_cols):
        if filename.endswith(".parquet"):
            self.is_parquet = True
            ## self._init_with_parquet(filename, text_col)
            return

        self.is_parquet = False
        self.bm25 = new _BM25(
                filename.encode("utf-8"),
                self.search_cols,
                self.min_df,
                self.max_df,
                self.k1,
                self.b,
                self.num_partitions,
                self.stopwords
                )

    cdef void _init_with_parquet(self, str filename, str text_col):
        from pyarrow import parquet as pq

        init = perf_counter()
        self.arrow_table = pq.ParquetFile(filename, memory_map=True).read()

        cdef list pydocs = self.arrow_table.column(text_col).to_pylist()

        cdef vector[vector[string]] docs
        cdef uint64_t num_docs = self.arrow_table.num_rows
        cdef uint64_t idx

        docs.reserve(num_docs)

        for idx in range(num_docs):
            docs.push_back(pydocs[idx].encode("utf-8"))

        self.bm25 = new _BM25(
                docs,
                self.min_df,
                self.max_df,
                self.k1,
                self.b,
                self.num_partitions,
                self.stopwords
                )
        print(f"Reading parquet file took {perf_counter() - init:.2f} seconds")

    cpdef get_topk_indices(
            self, 
            str query, 
            int query_max_df = INT_MAX, 
            int k = 10,
            list boost_factors = [] 
            ):
        if query is None:
            return [], []

        cdef vector[float] _boost_factors
        _boost_factors.reserve(len(boost_factors))
        for factor in boost_factors:
            _boost_factors.push_back(factor)

        cdef vector[BM25Result] results = self.bm25.query(
                query.upper().encode("utf-8"), 
                k, 
                query_max_df,
                _boost_factors
                )

        if results.size() == 0:
            return [], []

        cdef list scores  = []
        cdef list indices = []
        for result in results:
            scores.append(result.score)
            indices.append(result.doc_id)

        return scores, indices

    cdef list _get_topk_docs_parquet(
            self, 
            str query, 
            int k = 10, 
            int query_max_df = INT_MAX,
            list boost_factors = None
            ):
        if boost_factors is None:
            boost_factors = len(self.search_cols) * [1]

        cdef vector[float] _boost_factors
        _boost_factors.reserve(len(boost_factors))
        for factor in boost_factors:
            _boost_factors.push_back(factor)

        cdef vector[BM25Result] results = self.bm25.query(
                query.upper().encode("utf-8"), 
                k, 
                query_max_df,
                _boost_factors
                )

        if results.size() == 0:
            return []

        cdef list scores = []
        cdef list indices = []
        for result in results:
            scores.append(result.score)
            indices.append(result.doc_id)

        rows = self.arrow_table.take(indices).to_pylist()

        for idx, score in enumerate(scores):
            rows[idx]["score"] = score

        return rows


    def get_topk_docs(
            self, 
            str query, 
            int k = 10, 
            int query_max_df = INT_MAX,
            list boost_factors = None
            ):
        if self.is_parquet:
            return self._get_topk_docs_parquet(query, k, query_max_df)

        if boost_factors is None:
            boost_factors = len(self.search_cols) * [1]

        cdef vector[float] _boost_factors
        _boost_factors.reserve(len(boost_factors))
        for factor in boost_factors:
            _boost_factors.push_back(factor)

        cdef vector[vector[pair[string, string]]] results
        cdef list output = []
        cdef string _query = query.upper().encode("utf-8")

        if self.filename == "in_memory":
            raise RuntimeError("""
                Cannot get topk docs when documents were provided instead of a filename
            """)
        else:
            with nogil:
                results = self.bm25.get_topk_internal(
                        _query,
                        k, 
                        query_max_df,
                        _boost_factors
                        )

        for idx in range(len(results)):
            _dict = {}
            for jdx in range(len(results[idx])):
                _dict[results[idx][jdx].first.decode("utf-8")] = results[idx][jdx].second.decode("utf-8")

            output.append(_dict)

        return output
