#include <iostream>
#include <unistd.h>
#include <sys/stat.h>
#include <vector>
#include <queue>
#include <string>
#include <cstdint>
#include <cmath>
#include <fstream>
#include <sstream>
#include <omp.h>
#include <unistd.h>
#include <regex>

#include "engine.h"
#include "robin_hood.h"

#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>


static std::regex pattern("\"");

static void tokenize_whitespace_inplace(
		const std::string& str, 
		const std::function<void(const std::string&)>& callback
		) {
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

	size_t start = 0;
	size_t end   = 0;
	const size_t length = document.length();

	while ((end = document.find(' ', start)) != std::string::npos) {
        if (end != start) {
            tokenized_document.emplace_back(document.substr(start, end - start));
        }
        start = end + 1;
    }
    if (start < length) {
        tokenized_document.emplace_back(document.substr(start));
    }
    return tokenized_document;
}


void _BM25::read_csv(std::vector<uint32_t>& terms) {
	// Open the file
	FILE* file = fopen(csv_file.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << csv_file << std::endl;
		exit(1);
	}

	int search_column_index = -1;

	// Read the file line by line
	char*   line = NULL;
	size_t  len = 0;
	ssize_t read;
	int line_num = 0;

	// Get col names
	read = getline(&line, &len, file);
	std::istringstream iss(line);
	std::string value;
	while (std::getline(iss, value, ',')) {
		if (value.find("\n") != std::string::npos) {
			value.erase(value.find("\n"));
		}
		columns.push_back(value);
		if (value == search_col) {
			search_column_index = columns.size() - 1;
		}
	}

	if (search_column_index == -1) {
		std::cerr << "Search column not found in header" << std::endl;
		for (int i = 0; i < columns.size(); ++i) {
			std::cerr << columns[i] << ",";
		}
		std::cerr << std::endl;
		exit(1);
	}

	while ((read = getline(&line, &len, file)) != -1) {
		csv_line_offsets.push_back(ftell(file) - read);
		++line_num;

		// Split by commas not inside double quotes
		std::string line_str(line);
		std::vector<std::string> row;
		std::string value;
		std::string cell;
		bool in_quotes = false;
		for (int i = 0; i < line_str.size(); ++i) {
			if (line_str[i] == '"' && in_quotes) {
				row.push_back(cell);
				in_quotes = false;
				cell = "";
			}
			else if (line_str[i] == '"' && !in_quotes) {
				in_quotes = !in_quotes;
			}
			else if (line_str[i] == ',' && !in_quotes) {
				row.push_back(cell);
				cell = "";
			}
			else {
				cell += line_str[i];
			}
		}
		row.push_back(cell);

		// Add the search column to the documents without quotes
		std::string doc = row[search_column_index];
		std::transform(doc.begin(), doc.end(), doc.begin(), ::toupper);

		// Split doc on whitespace and update unique_term_mapping
		std::vector<std::string> doc_tokens = tokenize_whitespace(doc);
		for (const std::string& token : doc_tokens) {
			if (unique_term_mapping.find(token) == unique_term_mapping.end()) {
				uint32_t new_idx = unique_term_mapping.size();
				unique_term_mapping[token] = new_idx;
				terms.push_back(new_idx);
				continue;
			}

			terms.push_back(unique_term_mapping[token]);
		}
		doc_sizes.push_back(doc_tokens.size());
	}
	fclose(file);

	// Write the offsets to a file
	std::ofstream ofs(DIR_NAME + "/" + CSV_LINE_OFFSETS_NAME, std::ios::binary);
	ofs.write((char*)&csv_line_offsets[0], csv_line_offsets.size() * sizeof(uint32_t));
	ofs.close();

	num_docs = doc_sizes.size();
}

std::vector<std::pair<std::string, std::string>> _BM25::get_csv_line(int line_num) {
	std::ifstream file(csv_file);
	std::string line;
	file.seekg(csv_line_offsets[line_num]);
	std::getline(file, line);
	file.close();

	// Create effective json by combining column names with values split by commas
	std::vector<std::pair<std::string, std::string>> row;
	std::string cell;
	bool in_quotes = false;
	size_t col_idx = 0;

	for (size_t i = 0; i < line.size(); ++i) {
		if (line[i] == '"') {
			in_quotes = !in_quotes;
		}
		else if (line[i] == ',' && !in_quotes) {
			row.emplace_back(columns[col_idx], cell);
			cell.clear();
			++col_idx;
		}
		else {
			cell += line[i];
		}
	}
	row.emplace_back(columns[col_idx], cell);
	return row;
}

void _BM25::load_dbs_from_dir(std::string db_dir) {
	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status;

	const size_t cacheSize = 100 * 1048576; // 100 MB
	options.block_cache = leveldb::NewLRUCache(cacheSize);

	status = leveldb::DB::Open(options, db_dir + "/" + DOC_TERM_FREQS_DB_NAME, &doc_term_freqs_db);
	if (!status.ok()) {
		std::cerr << "Unable to open doc_term_freqs_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
	}

	status = leveldb::DB::Open(options, db_dir + "/" + INVERTED_INDEX_DB_NAME, &inverted_index_db);
	if (!status.ok()) {
		std::cerr << "Unable to open inverted_index_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
	}

	// Load the csv line offsets
	std::ifstream file(db_dir + "/" + CSV_LINE_OFFSETS_NAME);
	std::string line;
	while (std::getline(file, line)) {
		csv_line_offsets.push_back(std::stol(line));
	}
	file.close();
}

void _BM25::save_to_disk() {
	// Save:
	// 1. num_docs 
	// 2. min_df 
	// 3. max_df
	// 4. avg_doc_size
	// 5. k1 
	// 6. b
	// 7. cache_term_freqs 
	// 8. cache_inverted_index
	// 9. csv_line_offsets
	// 10. doc_sizes 
	// 11. large_dfs 
	// 13. inverted_index_db (if cache_inverted_index is true)
	// 14. doc_term_freqs_db (if cache_term_freqs is true)
	// 15. columns
	// 16. csv_file
	// 17. term_freq_line_offsets
	
}

void _BM25::init_dbs() {
	/*
	if (std::filesystem::exists(DIR_NAME)) {
		// Remove the directory if it exists
		std::filesystem::remove_all(DIR_NAME);
		std::filesystem::create_directory(DIR_NAME);
	}
	else {
		// Create the directory if it does not exist
		std::filesystem::create_directory(DIR_NAME);
	}
	*/
	// Use POSIX functions instead of std::filesystem
	if (access(DIR_NAME.c_str(), F_OK) != -1) {
		// Remove the directory if it exists
		std::string command = "rm -r " + DIR_NAME;
		system(command.c_str());

		// Create the directory
		command = "mkdir " + DIR_NAME;
		system(command.c_str());
	}
	else {
		// Create the directory if it does not exist
		std::string command = "mkdir " + DIR_NAME;
		system(command.c_str());
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

void _BM25::create_doc_term_freqs_db() {
	// Create term frequencies db
	leveldb::WriteOptions write_options;
	leveldb::Status status;
	leveldb::WriteBatch batch;
	for (const auto& pair : doc_term_freqs) {
		// batch.Put(pair.first, std::to_string(pair.second));
		batch.Put(std::to_string(pair.first), std::to_string(pair.second));
	}

	status = doc_term_freqs_db->Write(write_options, &batch);
	if (!status.ok()) {
		std::cerr << "Unable to write batch to doc_term_freqs_db" << std::endl;
		std::cerr << status.ToString() << std::endl;
	}
}

void _BM25::create_inverted_index_db() {
	// Create inverted index db
	leveldb::WriteOptions write_options;
	leveldb::Status status;

	leveldb::WriteBatch batch;
	for (const auto& pair : inverted_index) {
		std::string value;
		for (const auto& doc_id : pair.second) {
			value += std::to_string(doc_id) + " ";
		}
		// batch.Put(pair.first, value);
		batch.Put(std::to_string(pair.first), value);
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

// uint32_t _BM25::get_doc_term_freq_db(const std::string& term) {
uint32_t _BM25::get_doc_term_freq_db(const uint32_t& term_idx) {
	if (cache_doc_term_freqs) {
		return doc_term_freqs[term_idx];
	}

	leveldb::ReadOptions read_options;
	std::string value;
	leveldb::Status status = doc_term_freqs_db->Get(read_options, std::to_string(term_idx), &value);
	if (!status.ok()) {
		// If term not found, return 0
		return 0;
	}
	return std::stoi(value);
}

// std::vector<uint32_t> _BM25::get_inverted_index_db(const std::string& term) {
std::vector<uint32_t> _BM25::get_inverted_index_db(const uint32_t& term_idx) {
	if (cache_inverted_index) {
		return inverted_index[term_idx];
	}

	leveldb::ReadOptions read_options;
	std::string value;

	leveldb::Status status = inverted_index_db->Get(read_options, std::to_string(term_idx), &value);
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
			// fprintf(file, "%s %d\t", pair.first.c_str(), pair.second);
			fprintf(file, "%s %d\t", std::to_string(pair.first).c_str(), pair.second);
		}

		fprintf(file, "\n");
		offset = ftell(file);
	}
	fclose(file);
}

uint16_t _BM25::get_term_freq_from_file(
		int line_num,
		// const std::string& term
		const uint32_t& term_idx
		) {
	if (cache_term_freqs) {
		float tf = 0.0f;
		// for (const std::pair<std::string, uint16_t>& term_freq : term_freqs[line_num]) {
		for (const std::pair<uint32_t, uint16_t>& term_freq : term_freqs[line_num]) {
			if (term_freq.first == term_idx) {
				tf = term_freq.second;
				break;
			}
		}
		return tf;
	}
	return 0;

	/*
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
	*/
	return 0;
}


_BM25::_BM25(
		std::string csv_file,
		std::string search_col,
		int min_df,
		float max_df,
		float k1,
		float b,
		bool  cache_term_freqs,
		bool  cache_inverted_index,
		bool  cache_doc_term_freqs
		) : min_df(min_df), 
			max_df(max_df), 
			k1(k1), 
			b(b),
			cache_term_freqs(cache_term_freqs),
			cache_inverted_index(cache_inverted_index),
			cache_doc_term_freqs(cache_doc_term_freqs),
			search_col(search_col), 
			csv_file(csv_file) {

	auto overall_start = std::chrono::high_resolution_clock::now();
	
	// Read csv to get documents, line offsets, and columns
	// std::vector<std::string> documents;
	std::vector<uint32_t> terms;
	read_csv(terms);

	auto read_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> read_elapsed_seconds = read_end - overall_start;
	std::cout << "Read csv in " << read_elapsed_seconds.count() << " seconds" << std::endl;

	init_dbs();

	auto start = std::chrono::high_resolution_clock::now();

	// Accumulate document frequencies
	uint64_t terms_seen = 0;
	for (size_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		robin_hood::unordered_flat_set<uint32_t> seen_terms;

		uint16_t doc_size = doc_sizes[doc_id];
		size_t end = terms_seen + doc_size;

		for (size_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint32_t mapped_term_idx = terms[term_idx];

			if (doc_term_freqs.find(mapped_term_idx) == doc_term_freqs.end()) {
				doc_term_freqs[mapped_term_idx] = 1;
			}
			else {
				++doc_term_freqs[mapped_term_idx];
			}
			seen_terms.insert(mapped_term_idx);
			++terms_seen;
		}
	}
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished accumulating document frequencies in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();

	uint32_t max_number_of_occurrences = max_df * num_docs;
	robin_hood::unordered_set<uint32_t> blacklisted_terms;

	// Filter terms by min_df and max_df
	for (const robin_hood::pair<uint32_t, uint32_t>& term_count : doc_term_freqs) {
		if (term_count.second < min_df || term_count.second > max_number_of_occurrences) {
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
	term_freqs.resize(num_docs);

	start = std::chrono::high_resolution_clock::now();
	terms_seen = 0;
	for (uint32_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		uint16_t doc_size = doc_sizes[doc_id];

		size_t end = terms_seen + doc_size;
		for (size_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint32_t mapped_term_idx = terms[term_idx];
			++terms_seen;

			if (blacklisted_terms.find(mapped_term_idx) != blacklisted_terms.end()) {
				return;
			}


			// Use std::find_if to check if the term is already in the vector
			auto it = std::find_if(
					term_freqs[doc_id].begin(), 
					term_freqs[doc_id].end(), 
					[&](const std::pair<uint32_t, uint16_t>& p
					) {
				return p.first == mapped_term_idx;
			});

			if (it != term_freqs[doc_id].end()) {
				++it->second;
			}
			// tag this branch as likely
			else {
				term_freqs[doc_id].emplace_back(mapped_term_idx, 1);
				inverted_index[mapped_term_idx].push_back(doc_id);
			}
		}
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
	
	// Reserve here before potential clear.
	inverted_index.reserve(doc_term_freqs.size());

	start = std::chrono::high_resolution_clock::now();
	// Write doc_term_freqs to leveldb
	if (!cache_doc_term_freqs) {
		create_doc_term_freqs_db();
		doc_term_freqs.clear();
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished writing doc term frequencies to disk in " << elapsed_seconds.count() << "s" << std::endl;
	}


	/*
	start = std::chrono::high_resolution_clock::now();
	for (size_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		tokenize_whitespace_inplace(documents[doc_id], [&](const std::string& term) {
			if (blacklisted_terms.find(term) != blacklisted_terms.end()) {
				return;
			}
			inverted_index[term].push_back(doc_id);
		});
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;
	if (DEBUG) {
		std::cout << "Finished creating inverted index in " << elapsed_seconds.count() << "s" << std::endl;
	}
	*/

	start = std::chrono::high_resolution_clock::now();

	// Write inverted index to disk
	if (!cache_inverted_index) {
		create_inverted_index_db();
		inverted_index.clear();
	}

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

	// Save non-vector/map members to disk
	// save_to_disk();
}

_BM25::_BM25(
		std::string db_dir,
		std::string csv_file
		) {
	load_dbs_from_dir(db_dir);
}

// inline float _BM25::_compute_idf(const std::string& term) {
inline float _BM25::_compute_idf(const uint32_t& term_idx) {
	uint32_t df = get_doc_term_freq_db(term_idx);
	return log((num_docs - df + 0.5) / (df + 0.5));
}

inline float _BM25::_compute_bm25(
		// const std::string& term,
		const uint32_t& term_idx,
		uint32_t doc_id,
		float idf
		) {
	float tf = get_term_freq_from_file(doc_id, term_idx);
	float doc_size = doc_sizes[doc_id];

	return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_size / avg_doc_size));
}

std::vector<std::pair<uint32_t, float>> _BM25::query(
		std::string& query, 
		uint32_t k,
		uint32_t init_max_df 
		) {

	std::vector<std::string> tokenized_query;
	tokenized_query = tokenize_whitespace(query);

	std::vector<uint32_t> term_idxs;
	for (const std::string& term : tokenized_query) {
		if (unique_term_mapping.find(term) == unique_term_mapping.end()) {
			continue;
		}
		term_idxs.push_back(unique_term_mapping[term]);
	}

	if (term_idxs.size() == 0) {
		return std::vector<std::pair<uint32_t, float>>();
	}

	// Gather docs that contain at least one term from the query
	// Try using dynamic max_df for performance
	int local_max_df = init_max_df;
	robin_hood::unordered_set<uint32_t> candidate_docs;

	while (candidate_docs.size() == 0) {
		for (const uint32_t& term_idx : term_idxs) {
			std::vector<uint32_t> doc_ids = get_inverted_index_db(term_idx);

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
		local_max_df *= 20;

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
		// for (const std::string& term : tokenized_query) {
		for (const uint32_t& term_idx : term_idxs) {
			if (idx == 0) {
				idfs[jdx] = _compute_idf(term_idx);
			}
			score += _compute_bm25(term_idx, doc_id, idfs[jdx]);
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

std::vector<std::vector<std::pair<std::string, std::string>>> _BM25::get_topk_internal(
		std::string& _query,
		uint32_t top_k,
		uint32_t init_max_df
		) {
	std::vector<std::vector<std::pair<std::string, std::string>>> result;
	std::vector<std::pair<uint32_t, float>> top_k_docs = query(_query, top_k, init_max_df);
	result.reserve(top_k_docs.size());

	std::vector<std::pair<std::string, std::string>> row;
	for (int i = 0; i < top_k_docs.size(); ++i) {
		row = get_csv_line(top_k_docs[i].first);
		row.push_back(std::make_pair("score", std::to_string(top_k_docs[i].second)));
		result.push_back(row);
	}
	return result;
}
