#include <iostream>
#include <vector>
#include <queue>
#include <string>
#include <cstdint>
#include <cmath>
#include <omp.h>
#include <chrono>

#include "bm25_utils.h"
#include "robin_hood.h"

std::vector<std::string> tokenize_whitespace(
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

std::vector<std::string> tokenize_ngram(
		std::string& document, 
		int ngram_size
		) {
	std::vector<std::string> tokenized_document;

	if ((int)document.size() < ngram_size) {
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

	#pragma omp parallel for schedule(static)
	for (int idx = 0; idx < (int)documents.size(); ++idx) {
		tokenized_documents[idx] = tokenize_ngram(documents[idx], ngram_size);
	}
}

void init_members(
	std::vector<std::vector<std::string>>& tokenized_documents,
	robin_hood::unordered_map<std::string, std::vector<uint32_t>>& inverted_index,
	std::vector<robin_hood::unordered_map<std::string, uint16_t>>& term_freqs,
	robin_hood::unordered_map<std::string, uint32_t>& doc_term_freqs,
	std::vector<uint16_t>& doc_sizes,
	float& avg_doc_size,
	uint32_t& num_docs,
	int min_df,
	float max_df
	) {
	term_freqs.reserve(tokenized_documents.size());
	doc_sizes.reserve(tokenized_documents.size());

	int max_number_of_occurrences = (int)(max_df * tokenized_documents.size());

	auto start = std::chrono::high_resolution_clock::now();

	// Accumulate document frequencies
	for (const std::vector<std::string>& doc : tokenized_documents) {
		robin_hood::unordered_set<std::string> unique_terms;
		unique_terms.reserve(doc.size());

		for (const std::string& term : doc) {
			unique_terms.insert(term);
		}

		for (const std::string& term : unique_terms) {
			++doc_term_freqs[term];
		}
	}

	auto end = std::chrono::high_resolution_clock::now();

	std::chrono::duration<double> elapsed_seconds = end - start;
	std::cout << "Elapsed time for getting doc_term_freqs: " << elapsed_seconds.count() << "s\n";

	robin_hood::unordered_set<std::string> blacklisted_terms;

	// Filter terms by min_df and max_df
	for (const robin_hood::pair<std::string, uint32_t>& term_count : doc_term_freqs) {
		if ((int)term_count.second < min_df || (int)term_count.second > max_number_of_occurrences) {
			blacklisted_terms.insert(term_count.first);
			doc_term_freqs.erase(term_count.first);
		}
	}

	// Filter terms by min_df and max_df
	doc_sizes.resize(tokenized_documents.size());
	term_freqs.resize(tokenized_documents.size());

	num_docs = tokenized_documents.size();

	#pragma omp parallel for schedule(static)
	for (uint32_t doc_id = 0; doc_id < tokenized_documents.size(); ++doc_id) {
		const std::vector<std::string>& doc = tokenized_documents[doc_id];

		uint16_t doc_size = doc.size();
		doc_sizes[doc_id] = doc_size;

		avg_doc_size += doc_size;

		robin_hood::unordered_map<std::string, uint16_t> term_freq;
		for (const std::string& term : doc) {
			if (blacklisted_terms.find(term) != blacklisted_terms.end()) {
				continue;
			}
			++term_freq[term];
		}
		term_freqs[doc_id] = term_freq;
	}


	inverted_index.reserve(doc_term_freqs.size());

	for (uint32_t doc_id = 0; doc_id < tokenized_documents.size(); ++doc_id) {
		const std::vector<std::string>& doc = tokenized_documents[doc_id];
		for (const std::string& term : doc) {
			if (blacklisted_terms.find(term) != blacklisted_terms.end()) {
				continue;
			}
			inverted_index[term].push_back(doc_id);
		}
	}

	avg_doc_size /= num_docs;
}



_BM25::_BM25(
		std::vector<std::string>& documents,
		bool whitespace_tokenization,
		int ngram_size,
		int min_df,
		float max_df,
		float k1,
		float b
		) : whitespace_tokenization(whitespace_tokenization), 
			ngram_size(ngram_size), 
			min_df(min_df), 
			max_df(max_df), 
			k1(k1), 
			b(b) {
	std::vector<std::vector<std::string>> tokenized_documents;

	if (whitespace_tokenization) {
		tokenize_whitespace_batch(documents, tokenized_documents);
	} 
	else {
		tokenize_ngram_batch(documents, tokenized_documents, ngram_size);
	}

	init_members(
			tokenized_documents, 
			inverted_index, 
			term_freqs, 
			doc_term_freqs, 
			doc_sizes, 
			avg_doc_size, 
			num_docs, 
			min_df, 
			max_df
			);

	// Calc total memory usage
	uint64_t total_memory_usage = 0;
	total_memory_usage += sizeof(std::string) * documents.size();
	// total_memory_usage += sizeof(robin_hood::unordered_map<std::string, std::vector<uint32_t>>) * inverted_index.size();
	total_memory_usage += sizeof(robin_hood::unordered_map<std::string, robin_hood::unordered_set<uint32_t>>) * inverted_index.size();
	total_memory_usage += sizeof(robin_hood::unordered_map<std::string, uint32_t>) * term_freqs.size();
	total_memory_usage += sizeof(robin_hood::unordered_map<std::string, uint32_t>) * doc_term_freqs.size();
	total_memory_usage += sizeof(uint16_t) * doc_sizes.size();
	total_memory_usage += sizeof(float) * 2;
	total_memory_usage += sizeof(uint32_t);
	total_memory_usage += sizeof(int) * 4;

	std::cout << "Total memory usage: " << total_memory_usage / (1024 * 1024) << " MB" << std::endl;
}

inline float _BM25::_compute_idf(const std::string& term) {
	uint32_t df = doc_term_freqs[term];
	return log((num_docs - df + 0.5) / (df + 0.5));
}

inline float _BM25::_compute_bm25(
		const std::string& term,
		uint32_t doc_id
		) {
	float idf = _compute_idf(term);
	float tf  = term_freqs[doc_id][term];
	float doc_size = doc_sizes[doc_id];
	return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_size / avg_doc_size));
}

std::vector<std::pair<uint32_t, float>> _BM25::query(
		std::string& query, 
		uint32_t k 
		) {
	std::vector<std::string> tokenized_query;
	if (whitespace_tokenization) {
		tokenized_query = tokenize_whitespace(query);
	} 
	else {
		tokenized_query = tokenize_ngram(query, ngram_size);
	}

	// Gather docs that contain at least one term from the query
	/*
	robin_hood::unordered_set<uint32_t> candidate_docs;
	for (const std::string& term : tokenized_query) {
		if (inverted_index.find(term) == inverted_index.end()) {
			continue;
		}

		for (const uint32_t& doc_id : inverted_index[term]) {
			candidate_docs.insert(doc_id);
		}
	}
	*/

	// Try using dynamic max_df for performance
	int local_max_df = 1000;
	robin_hood::unordered_set<uint32_t> candidate_docs;

	while (candidate_docs.size() == 0) {
		for (const std::string& term : tokenized_query) {
			if (inverted_index.find(term) == inverted_index.end()) {
				continue;
			}

			if (inverted_index[term].size() > local_max_df) {
				continue;
			}

			for (const uint32_t& doc_id : inverted_index[term]) {
				candidate_docs.insert(doc_id);
			}
		}
		local_max_df *= 10;

		if (local_max_df > num_docs) {
			break;
		}
	}

	if (candidate_docs.size() == 0) {
		return std::vector<std::pair<uint32_t, float>>();
	}

	std::cout << "Num candidates: " << candidate_docs.size() << std::endl;

	// Priority queue to store top k docs
	// Largest to smallest scores
	struct _compare {
		bool operator()(const std::pair<uint32_t, float>& a, const std::pair<uint32_t, float>& b) {
			return a.second > b.second;
		}
	};
	std::priority_queue<
		std::pair<uint32_t, float>, 
		std::vector<std::pair<uint32_t, float>>, 
		_compare> top_k_docs;

	// Compute BM25 scores for each candidate doc
	for (const uint32_t& doc_id : candidate_docs) {
		float score = 0;
		for (const std::string& term : tokenized_query) {
			score += _compute_bm25(term, doc_id);
		}

		top_k_docs.push(std::make_pair(doc_id, score));
		if (top_k_docs.size() > k) {
			top_k_docs.pop();
		}
	}

	std::vector<std::pair<uint32_t, float>> result(top_k_docs.size());
	int idx = top_k_docs.size() - 1;
	while (!top_k_docs.empty()) {
		result[idx] = top_k_docs.top();
		top_k_docs.pop();
		--idx;
	}

	return result;
}
