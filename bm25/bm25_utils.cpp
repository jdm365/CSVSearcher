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

#include "bm25_utils.h"
#include "robin_hood.h"

#include "lmdb++.h"


static std::vector<int> line_offsets;
static void write_vector_to_file(
		const std::vector<robin_hood::unordered_flat_map<std::string, uint16_t>>& vec,
		const std::string& filename
		) {
	// Create memmap index to get specific line later
	std::ofstream file(filename);
	int offset = 0;
	for (const auto& map : vec) {
		line_offsets.push_back(offset);
		for (const auto& pair : map) {
			file << pair.first << " " << pair.second;
			file << "\t";
		}
		file << std::endl;
		offset = file.tellp();
	}
	file.close();
}

static uint16_t read_specific_line_frequency(
		const std::string& filename,
		int line_num,
		const std::string& term
		) {
	std::ifstream file(filename);
	file.seekg(line_offsets[line_num]);
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


static void store_map(const robin_hood::unordered_flat_map<std::string, uint32_t>& map) {
    // Create the directory if it does not exist
    std::filesystem::create_directory(DB_NAME);

	// Create db 
    auto env = lmdb::env::create();
    env.open(DB_NAME.c_str(), 0, 0664);

	env.set_mapsize(100LL * 1024 * 1024 * 1024);

    auto wtxn = lmdb::txn::begin(env);
    auto db   = lmdb::dbi::open(wtxn, nullptr);

    for (const auto& pair : map) {
		std::string value = std::to_string(pair.second);
        db.put(
				wtxn, 
				pair.first.c_str(), 
				value.c_str()
				);
    }

    wtxn.commit();
}

static uint32_t read_doc_frequency(const std::string& term) {
    auto env = lmdb::env::create();
    env.open(DB_NAME.c_str(), 0, 0664);
    auto rtxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto db   = lmdb::dbi::open(rtxn, nullptr);

    lmdb::val value;
    if (db.get(rtxn, lmdb::val(term.c_str()), value)) {
        uint32_t freq;
		// Will be a string
		// Use std::stoi to convert to int
		std::string value_str(reinterpret_cast<const char*>(value.data()), value.size());
		freq = std::stoi(value_str);
        return freq;
    }

    return 0;
}

/*
static std::string serialize_vector(const std::vector<uint32_t>& vec) {
	int size = vec.size() * sizeof(uint32_t);
	return std::string(
			reinterpret_cast<const char*>(vec.data()),
			size
			);
}

static std::vector<uint32_t> deserialize_vector(std::string data) {
	std::vector<uint32_t> vec;
	vec.resize(data.size() / sizeof(uint32_t));
	std::memcpy(vec.data(), data.data(), data.size());
	return vec;
}
*/
static std::string serialize_vector(const std::vector<uint32_t>& vec) {
	std::string data;
	for (const auto& val : vec) {
		data.append(std::to_string(val));
		data.append(",");
	}
	return data;
}

static std::vector<uint32_t> deserialize_vector(std::string data) {
	std::vector<uint32_t> vec;
	std::istringstream iss(data);
	std::string token;
	while (std::getline(iss, token, ',')) {
		vec.push_back(std::stoi(token));
	}
	return vec;
}

static void write_inverted_index(
		const robin_hood::unordered_flat_map<std::string, std::vector<uint32_t>>& inverted_index
		) {
	// Create the directory if it does not exist
	std::filesystem::create_directory(INVERTED_INDEX_DB_NAME);

	// Create db
	auto env = lmdb::env::create();
	env.open(INVERTED_INDEX_DB_NAME.c_str(), 0, 0664);

	env.set_mapsize(100LL * 1024 * 1024 * 1024);

	auto wtxn = lmdb::txn::begin(env, nullptr, 0);
	auto db   = lmdb::dbi::open(wtxn, nullptr, 0);

	for (const auto& pair : inverted_index) {
        const auto& key_str = pair.first;
        const auto& values = pair.second;

		std::string data = serialize_vector(values);

        db.put(
				wtxn, 
				key_str.c_str(),
				data.c_str()
				);
    }

    wtxn.commit();

}

static std::vector<uint32_t> read_inverted_index(const std::string& term) {
	auto env = lmdb::env::create();
	env.open(INVERTED_INDEX_DB_NAME.c_str(), 0, 0664);

	auto rtxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto db = lmdb::dbi::open(rtxn, nullptr);

	lmdb::val query(term.c_str());
	lmdb::val db_data;
	std::vector<uint32_t> value;

    // Attempt to get the data for the specified key
    if (db.get(rtxn, query, db_data)) {
		std::string value_string = std::string(
				reinterpret_cast<const char*>(db_data.data()),
				db_data.size()
				);
		value = deserialize_vector(
				value_string
				);
		return value;
    }

    rtxn.abort();
    return value;

}


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
	// robin_hood::unordered_flat_map<std::string, std::vector<uint32_t>>& inverted_index,
	robin_hood::unordered_flat_set<std::string>& large_dfs,
	std::vector<uint16_t>& doc_sizes,
	float& avg_doc_size,
	uint32_t& num_docs,
	int min_df,
	float max_df
	) {
	robin_hood::unordered_flat_map<std::string, std::vector<uint32_t>> inverted_index;

	std::vector<robin_hood::unordered_flat_map<std::string, uint16_t>> term_freqs;
	robin_hood::unordered_map<std::string, uint32_t> doc_term_freqs;
	term_freqs.reserve(tokenized_documents.size());
	doc_sizes.reserve(tokenized_documents.size());

	int max_number_of_occurrences = (int)(max_df * tokenized_documents.size());

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

	robin_hood::unordered_set<std::string> blacklisted_terms;

	// Filter terms by min_df and max_df
	for (const robin_hood::pair<std::string, uint32_t>& term_count : doc_term_freqs) {
		if (term_count.second > 5000) {
			large_dfs.insert(term_count.first);
		}

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

		robin_hood::unordered_map<std::string, uint16_t> term_freq;
		for (const std::string& term : doc) {
			if (blacklisted_terms.find(term) != blacklisted_terms.end()) {
				continue;
			}
			++term_freq[term];
		}
		term_freqs[doc_id] = term_freq;
	}

	// Write term_freqs to disk
	write_vector_to_file(term_freqs, "term_freqs.bin");

	// Write doc_term_freqs to lmdb
	store_map(doc_term_freqs);
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
	write_inverted_index(inverted_index);

	avg_doc_size = 0.0f;
	#pragma omp parallel for reduction(+:avg_doc_size)
	for (int doc_id = 0; doc_id < num_docs; ++doc_id) {
		avg_doc_size += (float)doc_sizes[doc_id];
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
			large_dfs,
			doc_sizes, 
			avg_doc_size, 
			num_docs, 
			min_df, 
			max_df
			);
}

inline float _BM25::_compute_idf(const std::string& term) {
	uint32_t df = read_doc_frequency(term);
	return log((num_docs - df + 0.5) / (df + 0.5));
}

inline float _BM25::_compute_bm25(
		const std::string& term,
		uint32_t doc_id,
		float idf
		) {
	float tf  = read_specific_line_frequency("term_freqs.bin", doc_id, term);
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
	int local_max_df = 5000;
	robin_hood::unordered_set<uint32_t> candidate_docs;

	while (candidate_docs.size() == 0) {
		for (const std::string& term : tokenized_query) {

			if (local_max_df == 5000) {
				if (large_dfs.find(term) != large_dfs.end()) {
					continue;
				}
			}
			std::vector<uint32_t> doc_ids = read_inverted_index(term);
			
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
