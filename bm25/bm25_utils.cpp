#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstdint>
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
		if (term_count.second >= min_df && term_count.second <= max_number_of_occurrences) {
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
