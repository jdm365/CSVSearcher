#include <cinttypes>
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
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>



void _BM25::init_dbs() {
	if (std::filesystem::exists(DIR_NAME)) {
		// Remove the directory if it exists
		std::filesystem::remove_all(DIR_NAME);
		std::filesystem::create_directory(DIR_NAME);
	}
	else {
		// Create the directory if it does not exist
		std::filesystem::create_directory(DIR_NAME);
	}

	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status;

	const size_t cacheSize = 100 * 1048576; // 100 MB
	options.block_cache = leveldb::NewLRUCache(cacheSize);

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
	leveldb::WriteBatch batch;
	for (const auto& pair : doc_term_freqs) {
		batch.Put(pair.first, std::to_string(pair.second));
	}

	status = doc_term_freqs_db->Write(write_options, &batch);
	if (!status.ok()) {
		std::cerr << "Unable to write batch to doc_term_freqs_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
	}
}

void _BM25::create_inverted_index_db(
		const robin_hood::unordered_map<std::string, std::vector<uint32_t>>& inverted_index
		) {
	// Create inverted index db
	leveldb::WriteOptions write_options;
	leveldb::Status status;

	leveldb::WriteBatch batch;
	for (const auto& pair : inverted_index) {
		std::string value;
		for (const auto& doc_id : pair.second) {
			value += std::to_string(doc_id) + " ";
		}
		batch.Put(pair.first, value);
	}

	status = inverted_index_db->Write(write_options, &batch);
	if (!status.ok()) {
		std::cerr << "Unable to write batch to inverted_index_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
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


void _BM25::write_term_freqs_to_file() {
	// Create memmap index to get specific line later
	term_freq_line_offsets.reserve(
			term_freqs.size()
			);

	// Use pure C to write to file
	FILE* file = fopen((DIR_NAME + "/" + TERM_FREQS_FILE_NAME).c_str(), "w");
	int offset = 0;

	for (const auto& map : term_freqs) {
		term_freq_line_offsets.push_back(offset);

		for (const auto& pair : map) {
			fprintf(file, "%s %d\t", pair.first.c_str(), pair.second);
		}

		fprintf(file, "\n");
		offset = ftell(file);
	}
	fclose(file);
}

uint16_t _BM25::get_term_freq_from_file(
		int line_num,
		const std::string& term
		) {
	if (cache_term_freqs) {
		float tf = 0.0f;
		for (const std::pair<std::string, uint16_t>& term_freq : term_freqs[line_num]) {
			if (term_freq.first == term) {
				tf = term_freq.second;
				break;
			}
		}
		return tf;
	}

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


static void tokenize_whitespace_inplace(const std::string& str, const std::function<void(const std::string&)>& callback) {
    size_t start = 0;
    size_t end = 0;

    while ((start = str.find_first_not_of(' ', end)) != std::string::npos) {
        end = str.find(' ', start);
        callback(str.substr(start, end - start));
    }
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


void tokenize_whitespace_batch(
		std::vector<std::string>& documents,
		std::vector<std::vector<std::string>>& tokenized_documents
		) {
	tokenized_documents.reserve(documents.size());
	for (const std::string& doc : documents) {
		tokenized_documents.push_back(tokenize_whitespace(doc));
	}
}

_BM25::_BM25(
		std::vector<std::string>& documents,
		int min_df,
		float max_df,
		float k1,
		float b,
		bool  cache_term_freqs
		) : min_df(min_df), 
			max_df(max_df), 
			k1(k1), 
			b(b),
			cache_term_freqs(cache_term_freqs) {

	auto overall_start = std::chrono::high_resolution_clock::now();

	init_dbs();

	if (DEBUG) {
		sleep(10);
	}

	auto start = std::chrono::high_resolution_clock::now();
	robin_hood::unordered_map<std::string, uint32_t> doc_term_freqs;
	doc_term_freqs.reserve(documents.size());

	// Accumulate document frequencies
	for (const std::string& _doc : documents) {
		SmallStringSet unique_terms;

		// Tokenize and process each term in the current document
		tokenize_whitespace_inplace(_doc, [&](const std::string& term) {
			unique_terms.insert(term);
		});

		// Update global frequencies based on the local counts
		for (const auto& term : unique_terms) {
			++doc_term_freqs[term];
		}
	}
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished accumulating document frequencies in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();
	// std::vector<std::vector<std::pair<std::string, uint16_t>>> term_freqs;

	int max_number_of_occurrences = (int)(max_df * documents.size());
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
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished filtering terms by min_df and max_df in " << elapsed_seconds.count() << "s" << std::endl;
	}

	// Filter terms by min_df and max_df
	num_docs = documents.size();

	doc_sizes.resize(num_docs);
	term_freqs.resize(num_docs);

	start = std::chrono::high_resolution_clock::now();
	for (uint32_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		std::vector<std::string> doc = tokenize_whitespace(documents[doc_id]);

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
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Got term frequencies in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();
	// Write term_freqs to disk
	if (!cache_term_freqs) {
		write_term_freqs_to_file();
		term_freqs.clear();
		term_freqs.shrink_to_fit();
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;
	if (DEBUG) {
		std::cout << "Finished writing term frequencies to disk in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();
	// Write doc_term_freqs to leveldb
	create_doc_term_freqs_db(doc_term_freqs);
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished writing doc term frequencies to disk in " << elapsed_seconds.count() << "s" << std::endl;
	}

	robin_hood::unordered_map<std::string, std::vector<uint32_t>> inverted_index;
	inverted_index.reserve(doc_term_freqs.size());

	start = std::chrono::high_resolution_clock::now();
	for (uint32_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		std::vector<std::string> doc = tokenize_whitespace(documents[doc_id]);
		for (const std::string& term : doc) {
			if (blacklisted_terms.find(term) != blacklisted_terms.end()) {
				continue;
			}
			inverted_index[term].push_back(doc_id);
		}
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;
	if (DEBUG) {
		std::cout << "Finished creating inverted index in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();

	// Write inverted index to disk
	create_inverted_index_db(inverted_index);

	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;
	if (DEBUG) {
		std::cout << "Finished writing inverted index to disk in " << elapsed_seconds.count() << "s" << std::endl;
	}

	avg_doc_size = 0.0f;
	for (int doc_id = 0; doc_id < num_docs; ++doc_id) {
		avg_doc_size += (float)doc_sizes[doc_id];
	}
	avg_doc_size /= num_docs;

	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - overall_start;
	if (DEBUG) {
		std::cout << "Finished creating BM25 index in " << elapsed_seconds.count() << "s" << std::endl;
	}
}

inline float _BM25::_compute_idf(const std::string& term) {
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
	tokenized_query = tokenize_whitespace(query);

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

	if (DEBUG) {
		std::cout << "Num candidates: " << candidate_docs.size() << std::endl;
	}

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
	std::vector<float> idfs(tokenized_query.size(), 0.0f);
	int idx = 0;
	for (const uint32_t& doc_id : candidate_docs) {
		float score = 0;
		int jdx = 0;
		for (const std::string& term : tokenized_query) {
			if (idx == 0) {
				idfs[jdx] = _compute_idf(term);
			}
			score += _compute_bm25(term, doc_id, idfs[jdx]);
			++jdx;
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
