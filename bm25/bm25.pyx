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

import datetime
import os
from time import perf_counter


cdef extern from "engine.h":
    cdef cppclass _BM25:
        _BM25(
                string filename,
                string search_col,
                int   min_df,
                float max_df,
                float k1,
                float b
                ) nogil
        _BM25(string db_dir)
        vector[pair[uint32_t, float]] query(string& term, uint32_t top_k, uint32_t init_max_df)
        vector[vector[pair[string, string]]] get_topk_internal(string& term, uint32_t k, uint32_t init_max_df)
        void save_to_disk()
        void load_from_disk(string db_dir)



cdef class BM25:
    cdef _BM25* bm25
    cdef int   min_df
    cdef float max_df
    cdef str   filename 
    cdef str   text_col
    cdef str   db_dir


    def __init__(
            self, 
            str  filename = None, 
            str  text_col = None,
            str  db_dir   = None,
            list documents = [], 
            int   min_df = 1,
            float max_df = 1.0
            ):
        self.filename = filename 
        self.text_col = text_col

        self.min_df = min_df
        self.max_df = max_df

        self.db_dir = 'bm25_db'

        if documents != []:
            pass

        '''
        if db_dir is not None:
            try:
                self.bm25 = new _BM25(db_dir.encode("utf-8"))
            except FileNotFoundError:
                print(f"DB dir {db_dir} not found. Has it moved? Training from scratch.")

                init = perf_counter()
                self._build_inverted_index(documents)
                print(f"Built index in {perf_counter() - init:.2f} seconds")


        elif filename != '' and text_col != '':
            init = perf_counter()
            self._build_inverted_index(documents)
            print(f"Built index in {perf_counter() - init:.2f} seconds")
        '''
        success = self.load()
        if not success:
            init = perf_counter()
            self._build_inverted_index(documents)
            print(f"Built index in {perf_counter() - init:.2f} seconds")

        '''
        else:
            raise ValueError("""One of the following must be provided: \
                                1: db_dir\
                                2: filename + text_col\
                                3: documents\
                                """)
        '''


    def __cinit__(
            self, 
            *args,
            **kwargs
            ):
        pass

    cdef bool load(self):
        ## First check if db_dir exists
        if not os.path.exists(self.db_dir):
            return False

        ## Check if db_dir has been modified since last save
        with open(os.path.join(self.db_dir, "last_modified.txt"), "r") as f:
            last_modified = f.read()

        if str(os.path.getmtime(self.db_dir)) != last_modified:
            return False

        with open(os.path.join(self.db_dir, "filename.txt"), "r") as f:
            last_filename = f.read()

        if self.filename != last_filename:
            return False

        ## Check if source_file has been modified since last save
        with open(os.path.join(self.db_dir, "last_modified_file.txt"), "r") as f:
            last_modified = f.read()

        if str(os.path.getmtime(self.filename)) != last_modified:
            return False

        self.bm25 = new _BM25(self.db_dir.encode("utf-8"))
        return True


    cdef save(self):
        ## self.bm25.save_to_disk(db_dir.encode("utf-8"))
        self.bm25.save_to_disk()

        ## Get from os when dir was last modified
        last_modified = os.path.getmtime(self.db_dir)

        ## Write to a file
        with open(os.path.join(self.db_dir, "last_modified.txt"), "w") as f:
            f.write(str(last_modified))

        ## Write to a file
        with open(os.path.join(self.db_dir, "filename.txt"), "w") as f:
            f.write(self.filename)

        last_modified = os.path.getmtime(self.filename)

        ## Write to a file
        with open(os.path.join(self.db_dir, "last_modified_file.txt"), "w") as f:
            f.write(str(last_modified))


    cdef void _build_inverted_index(self, list documents):
        self.bm25 = new _BM25(
                self.filename.encode("utf-8"),
                self.text_col.encode("utf-8"),
                self.min_df,
                self.max_df,
                1.2,
                0.4
                )
        ## self.bm25.save_to_disk()
        self.save()


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
