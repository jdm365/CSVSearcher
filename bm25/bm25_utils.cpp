#include <iostream>
#include <filesystem>
#include <vector>
#include <queue>
#include <string>
#include <cstdint>
#include <cmath>
#include <fstream>
#include <sstream>
#include <omp.h>
#include <unistd.h>

#include "bm25_utils.h"
#include "robin_hood.h"

#include <leveldb/db.h>


void _BM25::init_dbs() {
	// Create the directory if it does not exist
	std::filesystem::create_directory(DIR_NAME);

	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status;

	// const size_t cacheSize = 100 * 1048576; // 100 MB
	// options.block_cache = leveldb::NewLRUCache(cacheSize);

	status = leveldb::DB::Open(options, DIR_NAME + "/" + DOC_TERM_FREQS_DB_NAME, &doc_term_freqs_db);
	if (!status.ok()) {
		std::cerr << "Unable to open doc_term_freqs_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
	}

	status = leveldb::DB::Open(options, DIR_NAME + "/" + INVERTED_INDEX_DB_NAME, &inverted_index_db);
	if (!status.ok()) {
		std::cerr << "Unable to open inverted_index_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
	}
}

void _BM25::create_doc_term_freqs_db(
		const robin_hood::unordered_map<std::string, uint32_t>& doc_term_freqs
		) {
	// Create term frequencies db
	leveldb::WriteOptions write_options;
	leveldb::Status status;

	for (const auto& pair : doc_term_freqs) {
		status = doc_term_freqs_db->Put(write_options, pair.first, std::to_string(pair.second));
		if (!status.ok()) {
			std::cerr << "Unable to put key-value pair in term_freqs_db" << std::endl;
			std::cerr << status.ToString() << std::endl;
		}
	}
}

void _BM25::create_inverted_index_db(
		const robin_hood::unordered_map<std::string, std::vector<uint32_t>>& inverted_index
		) {
	// Create inverted index db
	leveldb::WriteOptions write_options;
	leveldb::Status status;

	for (const auto& pair : inverted_index) {
		std::string value;
		for (const auto& doc_id : pair.second) {
			value += std::to_string(doc_id) + " ";
		}
		status = inverted_index_db->Put(write_options, pair.first, value);
		if (!status.ok()) {
			std::cerr << "Unable to put key-value pair in inverted_index_db" << std::endl;
			std::cerr << status.ToString() << std::endl;
		}
	}
}

void _BM25::write_row_to_inverted_index_db(
		const std::string& term,
		uint32_t new_doc_id
		) {
	// Write new doc_id to inverted index db
	leveldb::WriteOptions write_options;
	leveldb::ReadOptions read_options;
	leveldb::Status status;

	std::string value;
	status = inverted_index_db->Get(read_options, term, &value);
	if (!status.ok()) {
		std::cerr << "Unable to get value from inverted_index_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
	}
	value += std::to_string(new_doc_id) + " ";
	status = inverted_index_db->Put(write_options, term, value);
}

uint32_t _BM25::get_doc_term_freq_db(const std::string& term) {
	leveldb::ReadOptions read_options;
	std::string value;
	leveldb::Status status = doc_term_freqs_db->Get(read_options, term, &value);
	if (!status.ok()) {
		// std::cerr << "Unable to get value from doc_term_freqs_db" << std::endl;
		// std::cerr << status.ToString() << std::endl;
		// If term not found, return 0
		return 0;
	}
	return std::stoi(value);
}

std::vector<uint32_t> _BM25::get_inverted_index_db(const std::string& term) {
	leveldb::ReadOptions read_options;
	std::string value;

	leveldb::Status status = inverted_index_db->Get(read_options, term, &value);
	if (!status.ok()) {
		// std::cerr << "Unable to get value from inverted_index_db" << std::endl;
		// std::cerr << status.ToString() << std::endl;
		// If term not found, return empty vector
		return std::vector<uint32_t>();
	}
	std::istringstream iss(value);
	std::vector<uint32_t> doc_ids;
	uint32_t doc_id;
	while (iss >> doc_id) {
		doc_ids.push_back(doc_id);
	}
	return doc_ids;
}


void _BM25::write_term_freqs_to_file(
		// const std::vector<robin_hood::unordered_map<std::string, uint16_t>>& vec
		const std::vector<std::vector<std::pair<std::string, uint16_t>>>& vec
		) {
	// Create memmap index to get specific line later
	std::ofstream file(DIR_NAME + "/" + TERM_FREQS_FILE_NAME);
	int offset = 0;
	for (const auto& map : vec) {
		term_freq_line_offsets.push_back(offset);
		for (const auto& pair : map) {
			file << pair.first << " " << pair.second;
			file << "\t";
		}
		file << std::endl;
		offset = file.tellp();
	}
	file.close();
}

uint16_t _BM25::get_term_freq_from_file(
		int line_num,
		const std::string& term
		) {
	std::ifstream file(DIR_NAME + "/" + TERM_FREQS_FILE_NAME);
	file.seekg(term_freq_line_offsets[line_num]);
	std::string line;
	std::getline(file, line);
	std::istringstream iss(line);
	std::string token;
	while (iss >> token) {
		if (token == term) {
			uint16_t freq;
			iss >> freq;
			return freq;
		}
	}
	return 0;
}


std::vector<std::string> tokenize_whitespace(
		const std::string& document
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
	for (const std::string& doc : documents) {
		tokenized_documents.emplace_back(tokenize_whitespace(doc));
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

_BM25::_BM25(
		// std::vector<std::string>& documents,
		std::vector<std::string> documents,
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

	init_dbs();
	std::vector<std::vector<std::string>> tokenized_documents;

	if (whitespace_tokenization) {
		tokenize_whitespace_batch(documents, tokenized_documents);
	} 
	else {
		tokenize_ngram_batch(documents, tokenized_documents, ngram_size);
	}

	std::cout << "Finished tokenizing documents" << std::endl;
	sleep(10);

	robin_hood::unordered_map<std::string, uint32_t> doc_term_freqs;
	doc_term_freqs.reserve(tokenized_documents.size());

	// Accumulate document frequencies
	for (const std::vector<std::string>& doc : tokenized_documents) {
		robin_hood::unordered_set<std::string> unique_terms;

		for (const std::string& term : doc) {
			unique_terms.insert(term);
		}

		for (const std::string& term : unique_terms) {
			++doc_term_freqs[term];
		}
	}

	std::cout << "Finished accumulating document frequencies" << std::endl;
	sleep(10);

	// std::vector<robin_hood::unordered_map<std::string, uint16_t>> term_freqs;
	std::vector<std::vector<std::pair<std::string, uint16_t>>> term_freqs;

	int max_number_of_occurrences = (int)(max_df * tokenized_documents.size());
	robin_hood::unordered_set<std::string> blacklisted_terms;

	// Filter terms by min_df and max_df
	for (const robin_hood::pair<std::string, uint32_t>& term_count : doc_term_freqs) {
		if (term_count.second > INIT_MAX_DF) {
			large_dfs.insert(term_count.first);
		}

		if ((int)term_count.second < min_df || (int)term_count.second > max_number_of_occurrences) {
			blacklisted_terms.insert(term_count.first);
			doc_term_freqs.erase(term_count.first);
		}
	}

	std::cout << "Finished filtering terms by min_df and max_df" << std::endl;
	sleep(10);

	// Filter terms by min_df and max_df
	num_docs = tokenized_documents.size();

	doc_sizes.resize(num_docs);
	term_freqs.resize(num_docs);

	#pragma omp parallel for schedule(static)
	for (uint32_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		const std::vector<std::string>& doc = tokenized_documents[doc_id];

		uint16_t doc_size = doc.size();
		doc_sizes[doc_id] = doc_size;

		robin_hood::unordered_map<std::string, uint16_t> term_freq;
		for (const std::string& term : doc) {
			if (blacklisted_terms.find(term) != blacklisted_terms.end()) {
				continue;
			}
			++term_freq[term];
		}
		std::vector<std::pair<std::string, uint16_t>> term_freq_vector;
		term_freq_vector.reserve(term_freq.size());
		for (const robin_hood::pair<std::string, uint16_t>& term_count : term_freq) {
			term_freq_vector.emplace_back(term_count.first, term_count.second);
		}
		term_freqs[doc_id] = term_freq_vector;
	}

	std::cout << "Got term frequencies" << std::endl;
	sleep(10);

	// Write term_freqs to disk
	write_term_freqs_to_file(term_freqs);

	// Write doc_term_freqs to lmdb
	create_doc_term_freqs_db(doc_term_freqs);

	std::cout << "Finished writing term frequencies to disk" << std::endl;
	sleep(10);

	robin_hood::unordered_map<std::string, std::vector<uint32_t>> inverted_index;
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
	// Write inverted index to disk
	create_inverted_index_db(inverted_index);
	/*
	for (uint32_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		const std::vector<std::string>& doc = tokenized_documents[doc_id];
		for (const std::string& term : doc) {
			if (blacklisted_terms.find(term) != blacklisted_terms.end()) {
				continue;
			}
			write_row_to_inverted_index_db(term, doc_id);
		}
		if (doc_id % 10000 == 0) {
			std::cout << "Finished writing " << doc_id << "/" << num_docs << " rows to inverted index" << std::endl;
		}
	}
	*/

	std::cout << "Finished writing inverted index to disk" << std::endl;
	sleep(10);

	avg_doc_size = 0.0f;
	#pragma omp parallel for reduction(+:avg_doc_size)
	for (int doc_id = 0; doc_id < num_docs; ++doc_id) {
		avg_doc_size += (float)doc_sizes[doc_id];
	}
	avg_doc_size /= num_docs;
}

inline float _BM25::_compute_idf(const std::string& term) {
	// uint32_t df = read_doc_frequency(term);
	uint32_t df = get_doc_term_freq_db(term);
	return log((num_docs - df + 0.5) / (df + 0.5));
}

inline float _BM25::_compute_bm25(
		const std::string& term,
		uint32_t doc_id,
		float idf
		) {
	float tf = get_term_freq_from_file(doc_id, term);
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
	// Try using dynamic max_df for performance
	int local_max_df = INIT_MAX_DF;
	robin_hood::unordered_set<uint32_t> candidate_docs;

	while (candidate_docs.size() == 0) {
		for (const std::string& term : tokenized_query) {

			if (local_max_df == INIT_MAX_DF) {
				if (large_dfs.find(term) != large_dfs.end()) {
					continue;
				}
			}
			std::vector<uint32_t> doc_ids = get_inverted_index_db(term);

			if (doc_ids.size() == 0) {
				continue;
			}

			if (doc_ids.size() > local_max_df) {
				continue;
			}

			for (const uint32_t& doc_id : doc_ids) {
				candidate_docs.insert(doc_id);
			}
		}
		local_max_df *= 5;

		if (local_max_df > num_docs || local_max_df > (int)max_df * num_docs) {
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
	std::vector<float> idfs(tokenized_query.size());

	int idx = 0;
	for (const uint32_t& doc_id : candidate_docs) {
		float score = 0;
		for (int jdx = 0; jdx < tokenized_query.size(); ++jdx) {
			const std::string& term = tokenized_query[jdx];
			if (idx == 0) {
				idfs[jdx] = _compute_idf(term);
			}
			score += _compute_bm25(term, doc_id, idfs[jdx]);
		}

		top_k_docs.push(std::make_pair(doc_id, score));
		if (top_k_docs.size() > k) {
			top_k_docs.pop();
		}
		++idx;
	}

	std::vector<std::pair<uint32_t, float>> result(top_k_docs.size());
	idx = top_k_docs.size() - 1;
	while (!top_k_docs.empty()) {
		result[idx] = top_k_docs.top();
		top_k_docs.pop();
		--idx;
	}

	return result;
}
