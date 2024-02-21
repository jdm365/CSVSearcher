# cython: language_level=3

cimport cython

from libc.stdint cimport uint64_t, uint32_t, uint16_t
from libc.stdlib cimport malloc, free
from libc.stdio cimport FILE, fopen, fclose, fgets
from libc.string cimport strlen, strtok
from cpython.array cimport array
from libc.math cimport log
from typing import List

from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.queue cimport priority_queue
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp cimport bool

cimport numpy as np
import numpy as np
np.import_array()
import gc
import re

from time import perf_counter
import sys


cdef extern from "bm25_utils.h":
    cdef cppclass _BM25:
        _BM25(
                vector[string]& documents,
                int   min_df,
                float max_df,
                float k1,
                float b,
                bool  cache_term_freqs
                ) nogil
        vector[pair[uint32_t, float]] query(string& term, uint32_t doc_id)



cdef class BM25:
    cdef _BM25* bm25
    cdef object data_index
    cdef list  cols
    cdef int   min_df
    cdef float max_df
    cdef bool  cache_term_freqs


    def __init__(
            self, 
            List[str] documents = [], 
            str csv_file = '', 
            str text_column = '',
            int min_df = 1,
            float max_df = 1,
            bool cache_term_freqs = True
            ):
        self.cache_term_freqs = cache_term_freqs

        self.min_df = min_df
        self.max_df = max_df

        if documents != []:
            pass

        elif csv_file != '' and text_column != '':
            init = perf_counter()
            from indxr import Indxr

            self.data_index = Indxr(
                    csv_file,
                    return_dict=False,
                    has_header=True
                    )
            print(f"Built data index in {perf_counter() - init:.2f} seconds")

            ## Extract documents from the csv file
            ## Find column index
            with open(csv_file, "r") as f:
                header = f.readline().strip().split(",")
                text_column_index = header.index(text_column)

                self.cols = header

            remove_commas = lambda x: x.group(0).replace(",", "")
            pattern = r'"[^\"]*"'
            compiled = re.compile(pattern)

            def clean(x):
                return re.sub(compiled, remove_commas, x).replace("\"", "")\
                        .lower().split(",")

            init = perf_counter()
            documents = []
            with open(csv_file, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    if idx == 0:
                        continue

                    documents.append(
                            clean(line)[text_column_index]
                            )
            print(f"Extracted documents in {perf_counter() - init:.2f} seconds")


            init = perf_counter()
            self._build_inverted_index(documents)
            print(f"Built index in {perf_counter() - init:.2f} seconds")
            '''
            self.bm25 = new _BM25(
                    csv_file.encode("utf-8"),
                    text_column_index,
                    self.whitespace_tokenization,
                    self.ngram_size,
                    self.min_df,
                    self.max_df,
                    1.2,
                    0.4
                    )
            '''

            del documents
            gc.collect()
        else:
            raise ValueError("Either documents or csv_file and text_column must be provided")

        ## Call self.data_index[0] once. First call is slow.
        self.data_index[0]


    def __cinit__(
            self, 
            *args,
            **kwargs
            ):
        pass

    cdef void _build_inverted_index(self, list documents):
        cdef vector[string] vector_documents
        vector_documents.reserve(len(documents))

        for idx, doc in enumerate(documents):
            vector_documents.push_back(doc.encode("utf-8"))

        self.bm25 = new _BM25(
                vector_documents,
                self.min_df,
                self.max_df,
                1.2,
                0.4,
                self.cache_term_freqs
                )

        vector_documents.clear()
        vector_documents.shrink_to_fit()
        

    def get_topk_docs(self, str query, int k = 10):
        cdef list scores   = []
        cdef list topk_ids = []
        cdef vector[pair[uint32_t, float]] results
        cdef list output

        results = self.bm25.query(query.lower().encode("utf-8"), k)

        k = min(k, len(results))

        for idx in range(k):
            topk_ids.append(results[idx].first)
            scores.append(results[idx].second)
            
        output = []
        ## fields = self.data_index[topk_ids]
        fields = self.data_index.mget(topk_ids)
        for idx, data_idx in enumerate(topk_ids):
            ## doc = dict(zip(self.cols, self.data_index[data_idx]))
            doc = dict(zip(self.cols, fields[idx]))
            doc["score"] = scores[idx]
            output.append(doc)
        return output 


    def query(self, str query):
        results = self.bm25.query(query.lower().encode("utf-8"), 10)

        scores  = []
        indices = []
        for idx, score in results:
            scores.append(score)
            indices.append(idx)

        return scores, indices
