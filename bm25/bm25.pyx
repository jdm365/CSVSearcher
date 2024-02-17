# cython: language_level=3

cimport cython

from libc.stdint cimport uint64_t, uint32_t, uint16_t
from libc.stdlib cimport malloc, free
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

from time import perf_counter
import sys


cdef extern from "robin_hood.h" namespace "robin_hood":
    cdef cppclass robin_hood_map[string, uint32_t]:
        robin_hood_map()
        void reserve(size_t n)
        void insert(pair[string, uint32_t] item)
        size_t size()
        void clear()

cdef extern from *:
    """
    #include <iostream>
    #include <vector>
    #include <unordered_map>
    #include <unordered_set>
    #include <string>
    #include <omp.h>

    #include "robin_hood.h"

    inline std::vector<std::string> tokenize_whitespace(
            std::string& document
            ) {
        std::vector<std::string> tokenized_document;

        std::string token;
        for (char c : document) {
            if (c == ' ') {
                if (!token.empty()) {
                    tokenized_document.push_back(token);
                    token.clear();
                }
                continue;
            }
            token.push_back(c);
        }

        if (!token.empty()) {
            tokenized_document.push_back(token);
        }

        return tokenized_document;
    }

    inline std::vector<std::string> tokenize_ngram(
            std::string& document, 
            int ngram_size
            ) {
        std::vector<std::string> tokenized_document;

        if (document.size() < ngram_size) {
            if (!document.empty()) {
                tokenized_document.push_back(document);
            }
            return tokenized_document;
        }

        std::string token;
        for (int i = 0; i < (int)document.size() - ngram_size + 1; ++i) {
            token = document.substr(i, ngram_size);
            tokenized_document.push_back(token);
        }
        return tokenized_document;
    }


    void tokenize_whitespace_batch(
            std::vector<std::string>& documents,
            std::vector<std::vector<std::string>>& tokenized_documents
            ) {
        tokenized_documents.reserve(documents.size());
        for (std::string& doc : documents) {
            tokenized_documents.push_back(tokenize_whitespace(doc));
        }
    }

    void tokenize_ngram_batch(
            std::vector<std::string>& documents,
            std::vector<std::vector<std::string>>& tokenized_documents,
            int ngram_size
            ) {
        tokenized_documents.resize(documents.size());

        // Set num threads to max threads
        omp_set_num_threads(omp_get_max_threads());

        std::cout << "Num threads: " << omp_get_max_threads() << std::endl;

        #pragma omp parallel for schedule(static)
        for (int idx = 0; idx < (int)documents.size(); ++idx) {
            tokenized_documents[idx] = tokenize_ngram(documents[idx], ngram_size);
        }
    }

    void init_members(
        std::vector<std::vector<std::string>>& tokenized_documents,
        std::unordered_map<std::string, std::vector<uint32_t>>& inverted_index,
        std::vector<std::unordered_map<std::string, uint32_t>>& term_freqs,
        std::unordered_map<std::string, uint32_t>& doc_term_freqs,
        std::vector<uint16_t>& doc_sizes,
        float& avg_doc_size,
        uint32_t& num_docs,
        int min_df,
        float max_df
        ) {
        uint32_t doc_id = 0;
        std::unordered_set<std::string> unique_terms;

        term_freqs.reserve(tokenized_documents.size());
        doc_sizes.reserve(tokenized_documents.size());

        int max_number_of_occurrences = (int)(max_df * tokenized_documents.size());

        std::cout << "Preparing " << tokenized_documents.size() << " documents" << std::endl;

        std::unordered_map<std::string, uint32_t> term_counts;
        term_counts.reserve(tokenized_documents.size());

        for (const std::vector<std::string>& doc : tokenized_documents) {
            for (const std::string& term : doc) {
                if (term_counts.find(term) == term_counts.end()) {
                    term_counts[term] = 1;
                } 
                else {
                    ++term_counts[term];
                }
            }
        }

        // Filter terms by min_df and max_df
        std::unordered_set<std::string> filter_terms;

        for (const std::pair<std::string, uint32_t>& term_count : term_counts) {
            if (term_count.second >= min_df && term_count.second <= max_number_of_occurrences) {
                // std::cout << "Adding term: " << term_count.first << " with count: " << term_count.second << std::endl;
                filter_terms.insert(term_count.first);
            }
        }

        for (std::vector<std::string>& doc : tokenized_documents) {
            uint16_t doc_size = doc.size();

            doc_sizes.push_back(doc_size);
            avg_doc_size += doc_size;
            ++num_docs;

            std::unordered_map<std::string, uint32_t> term_freq;
            unique_terms.clear();

            for (std::string& term : doc) {
                if (filter_terms.find(term) == filter_terms.end()) {
                    continue;
                }
                ++term_freq[term];
                unique_terms.insert(term);
            }
            term_freqs.push_back(term_freq);

            for (const std::string& term : unique_terms) {
                ++doc_term_freqs[term];
            }

            for (const std::string& term : unique_terms) {
                inverted_index[term].push_back(doc_id);
            }
            ++doc_id;
        }

        avg_doc_size /= num_docs;
    }
    """
    vector[string] tokenize_whitespace(
            string& document
            )
    vector[string] tokenize_ngram(
            string& document, 
            int ngram_size
            )
    void tokenize_whitespace_batch(
            vector[string]& documents,
            vector[vector[string]]& tokenized_documents
            )
    void tokenize_ngram_batch(
            vector[string]& documents,
            vector[vector[string]]& tokenized_documents,
            int ngram_size
            ) nogil
    void init_members(
        vector[vector[string]]& tokenized_documents,
        unordered_map[string, vector[uint32_t]]& inverted_index,
        vector[unordered_map[string, uint32_t]]& term_freqs,
        unordered_map[string, uint32_t]& doc_term_freqs,
        vector[uint16_t]& doc_sizes,
        float& avg_doc_size,
        uint32_t& num_docs,
        int min_df,
        float max_df
        )


cdef class BM25:
    cdef unordered_map[string, vector[uint32_t]] inverted_index
    cdef vector[unordered_map[string, uint32_t]] term_freqs
    cdef unordered_map[string, uint32_t] doc_term_freqs
    cdef vector[uint16_t] doc_sizes
    cdef float avg_doc_size
    cdef uint32_t num_docs
    cdef object data_index
    cdef list cols
    cdef bool whitespace_tokenization
    cdef int ngram_size
    cdef int min_df
    cdef float max_df

    def __init__(
            self, 
            List[str] documents = [], 
            str csv_file = '', 
            str text_column = '',
            bool whitespace_tokenization = True,
            int ngram_size = 3,
            int min_df = 1,
            float max_df = 0.1,
            ):
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

            ## Extract documents from the csv file
            ## Find column index
            with open(csv_file, "r") as f:
                header = f.readline().strip().split(",")
                text_column_index = header.index(text_column)

                self.cols = header

            documents = []
            with open(csv_file, "r") as f:
                for line in f:
                    documents.append(line.strip().split(",")[text_column_index].lower())

            print(f"Built data index in {perf_counter() - init:.2f} seconds")
        else:
            raise ValueError("Either documents or csv_file and text_column must be provided")

        self.whitespace_tokenization = whitespace_tokenization
        self.ngram_size = ngram_size

        self.min_df = min_df
        self.max_df = max_df

        init = perf_counter()
        self._build_inverted_index(documents)
        print(f"Built index in {perf_counter() - init:.2f} seconds")

        ## Call self.data_index[0] once
        self.data_index[0]


    def __cinit__(
            self, 
            *args,
            **kwargs
            ):
        self.inverted_index = unordered_map[string, vector[uint32_t]]()
        self.term_freqs     = vector[unordered_map[string, uint32_t]]()
        self.doc_term_freqs = unordered_map[string, uint32_t]()
        self.doc_sizes      = vector[uint16_t]()
        self.avg_doc_size   = 0.0
        self.num_docs       = 0


    cdef void _build_inverted_index(self, list documents):
        cdef uint32_t doc_id = 0
        cdef uint16_t doc_size
        cdef string term
        cdef uint32_t term_freq
        cdef vector[string] document

        self.num_docs = len(documents)

        cdef vector[string] vector_documents
        cdef vector[vector[string]] tokenized_documents

        for idx, doc in enumerate(documents):
            vector_documents.push_back(doc.encode("utf-8"))

        init = perf_counter()
        if self.whitespace_tokenization:
            tokenize_whitespace_batch(
                    vector_documents,
                    tokenized_documents
                    )
        else:
            tokenize_ngram_batch(
                    vector_documents,
                    tokenized_documents, 
                    self.ngram_size
                    )
        print(f"Tokenized documents in {perf_counter() - init:.2f} seconds")
        print(tokenized_documents.size())

        '''
        for document in tokenized_documents:
            doc_size = document.size()

            self.doc_sizes.push_back(doc_size)
            self.avg_doc_size += doc_size

            term_freqs = unordered_map[string, uint32_t]()
            for term in document:

                if term_freqs.find(term) == term_freqs.end():
                    term_freqs[term] = 1

                    if self.doc_term_freqs.find(term) == self.doc_term_freqs.end():
                        self.doc_term_freqs[term] = 1
                    else:
                        self.doc_term_freqs[term] += 1
                else:
                    term_freqs[term] += 1

                self.inverted_index[term].push_back(doc_id)
                if term == b"netflix":
                    print(term, doc_id, self.inverted_index[term])

            self.term_freqs.push_back(term_freqs)
            doc_id += 1

        self.avg_doc_size /= len(documents)
        '''
        init = perf_counter()
        print(self.min_df, self.max_df)
        init_members(
                tokenized_documents,
                self.inverted_index,
                self.term_freqs,
                self.doc_term_freqs,
                self.doc_sizes,
                self.avg_doc_size,
                self.num_docs,
                self.min_df,
                self.max_df
                )
        print(f"Initialized members in {perf_counter() - init:.2f} seconds")


    cpdef float _compute_idf(self, string term):
        cdef float idf
        cdef uint32_t doc_freq

        doc_freq = self.doc_term_freqs[term]
        idf = log(self.num_docs - doc_freq + 0.5) / (doc_freq + 0.5)

        return idf

    cdef float _compute_bm25(
            self, 
            string term, 
            uint32_t doc_id, 
            float k1 = 1.5, 
            float b = 0.75
            ):

        cdef float idf
        cdef float tf
        cdef float doc_size
        cdef float avg_doc_size
        cdef float bm25
        cdef uint32_t term_freq

        idf = self._compute_idf(term)
        if self.term_freqs[doc_id].find(term) == self.term_freqs[doc_id].end():
            term_freq = 0
        else:
            term_freq = self.term_freqs[doc_id][term]

        doc_size = self.doc_sizes[doc_id]
        avg_doc_size = self.avg_doc_size

        bm25 = idf * (term_freq * (k1 + 1)) / (term_freq + k1 * (1 - b + b * doc_size / avg_doc_size))

        return bm25

    cpdef query(self, str query, int k = 10):
        cdef vector[string] query_terms

        if self.whitespace_tokenization:
            query_terms = tokenize_whitespace(query.lower().encode("utf-8"))
        else:
            query_terms = tokenize_ngram(query.lower().encode("utf-8"), self.ngram_size)

        cdef string term
        cdef uint32_t doc_id
        cdef uint32_t term_freq
        cdef float score
        cdef unordered_set[uint32_t] doc_ids
        cdef priority_queue[pair[float, uint32_t]] top_k_scores

        ## Gather doc_ids
        for term in query_terms:
            for doc_id in self.inverted_index[term]:
                doc_ids.insert(doc_id)

        print(f"{len(doc_ids)} candidate documents")

        k = min(k, doc_ids.size())

        ## Compute BM25 scores
        for doc_id in doc_ids:
            score = 0.0
            for term in query_terms:
                score += self._compute_bm25(term, doc_id)
            top_k_scores.push(pair[float, uint32_t](score, doc_id))

        ## Return top k scores and doc_ids
        cdef np.ndarray[float, ndim=1] scores      = np.zeros(k, dtype=np.float32)
        cdef np.ndarray[uint32_t, ndim=1] topk_ids = np.zeros(k, dtype=np.uint32)

        for i in range(k):
            scores[i] = top_k_scores.top().first
            topk_ids[i] = top_k_scores.top().second
            top_k_scores.pop()

        return scores, topk_ids

    def get_topk_docs(self, str query, int k = 10):
        cdef np.ndarray[float, ndim=1] scores
        cdef np.ndarray[uint32_t, ndim=1] topk_ids

        results = self.query(query, k)
        scores = results[0]
        topk_ids = results[1]

        return [dict(zip(self.cols, self.data_index[i - 1])) for i in topk_ids]

