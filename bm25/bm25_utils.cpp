#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstdint>
#include <cmath>
#include <omp.h>

#include "bm25_utils.h"

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
		if ((int)term_count.second >= min_df && (int)term_count.second <= max_number_of_occurrences) {
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

	std::priority_queue<
		std::pair<uint32_t, float>, 
		std::vector<std::pair<uint32_t, float>>, 
		std::greater<std::pair<uint32_t, float>>
		> top_k_docs;

	std::unordered_map<uint32_t, float> doc_scores;
	for (const std::string& term : tokenized_query) {
		if (inverted_index.find(term) == inverted_index.end()) {
			continue;
		}
		for (uint32_t& doc_id : inverted_index[term]) {
			doc_scores[doc_id] += _compute_bm25(term, doc_id);
		}
	}

	for (const std::pair<uint32_t, float>& doc_score : doc_scores) {
		top_k_docs.push(doc_score);
		if (top_k_docs.size() > k) {
			top_k_docs.pop();
		}
	}

	std::vector<std::pair<uint32_t, float>> result;
	result.reserve(top_k_docs.size());

	while (!top_k_docs.empty()) {
		result.push_back(top_k_docs.top());
		top_k_docs.pop();
	}

	return result;
}
