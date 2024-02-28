# cython: language_level=3

cimport cython

from libc.stdint cimport uint32_t 

from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.pair cimport pair
from libcpp cimport bool

cimport numpy as np
import numpy as np
np.import_array()

from time import perf_counter


cdef extern from "engine.h":
    cdef cppclass _BM25:
        _BM25(
                string csv_file,
                string search_col,
                int   min_df,
                float max_df,
                float k1,
                float b,
                bool  cache_term_freqs,
                bool  cache_inverted_index,
                bool  cache_doc_term_freqs
                ) nogil
        vector[pair[uint32_t, float]] query(string& term, uint32_t top_k, uint32_t init_max_df)
        vector[vector[pair[string, string]]] get_topk_internal(string& term, uint32_t k, uint32_t init_max_df)



cdef class BM25:
    cdef _BM25* bm25
    cdef int   min_df
    cdef float max_df
    cdef bool  cache_term_freqs
    cdef bool  cache_inverted_index
    cdef bool  cache_doc_term_freqs
    cdef str   csv_file
    cdef str   text_col


    def __init__(
            self, 
            str  csv_file, 
            str  text_col,
            list documents = [], 
            int   min_df = 1,
            float max_df = 1,
            bool cache_term_freqs = True,
            bool cache_inverted_index = True,
            bool cache_doc_term_freqs = False 
            ):
        self.cache_term_freqs     = cache_term_freqs
        self.cache_inverted_index = cache_inverted_index
        self.cache_doc_term_freqs = cache_doc_term_freqs
        self.csv_file = csv_file
        self.text_col = text_col

        self.min_df = min_df
        self.max_df = max_df

        if documents != []:
            pass

        elif csv_file != '' and text_col != '':
            init = perf_counter()

            init = perf_counter()
            self._build_inverted_index(documents)
            print(f"Built index in {perf_counter() - init:.2f} seconds")

        else:
            raise ValueError("Either documents or csv_file and text_column must be provided")


    def __cinit__(
            self, 
            *args,
            **kwargs
            ):
        pass

    cdef void _build_inverted_index(self, list documents):
        self.bm25 = new _BM25(
                self.csv_file.encode("utf-8"),
                self.text_col.encode("utf-8"),
                self.min_df,
                self.max_df,
                1.2,
                0.4,
                self.cache_term_freqs,
                self.cache_inverted_index,
                self.cache_doc_term_freqs
                )


    def query(self, str query, int init_max_df = 1000):
        results = self.bm25.query(query.upper().encode("utf-8"), 10, init_max_df)

        scores  = []
        indices = []
        for idx, score in results:
            scores.append(score)
            indices.append(idx)

        return scores, indices


    def get_topk_docs(self, str query, int k = 10, int init_max_df = 1000):
        cdef vector[vector[pair[string, string]]] results
        cdef list output = []

        results = self.bm25.get_topk_internal(query.upper().encode("utf-8"), k, init_max_df)

        for idx in range(len(results)):
            _dict = {}
            for jdx in range(len(results[idx])):
                _dict[results[idx][jdx].first.decode("utf-8")] = results[idx][jdx].second.decode("utf-8")

            output.append(_dict)

        return output
