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
#include <ctype.h>

#include "engine.h"
#include "robin_hood.h"


void _BM25::read_json(std::vector<uint32_t>& terms) {
	// Open the file
	FILE* file = fopen(filename.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << filename << std::endl;
		exit(1);
	}

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;

	while ((read = getline(&line, &len, file)) != -1) {
		line_offsets.push_back(ftello(file) - read);
		++line_num;

		// Iterate of line chars until we get to relevant column.
		int char_idx   = 0;
		int field_idx  = 0;
		while (true) {
			start:
			if (line[char_idx] == '"') {
				// Found first key. Match against search_col
				++char_idx;

				for (const char& c : search_col) {
					if (c != line[char_idx]) {
						// Scan to next key and then goto start.
						// Basically count until four unescaped quotes
						// are found the goto start
						size_t num_quotes = 0;
						while (num_quotes < 4) {
							num_quotes += (line[char_idx] == '"');
							++char_idx;
						}
						--char_idx;
						goto start;
					}
					++char_idx;
				}

				if (line[char_idx] == '"') {
					// If we made it here we found the correct key.
					// Now iterate just past 1 more unescaped num_quotes
					// to get to the value.
					++char_idx;
					while (line[char_idx] != '"') {
						++char_idx;
					}
					++char_idx;
					break;
				}
			}
			else if (line[char_idx] == '}') {
				std::cout << "Search field not found on line: " << line_num << std::endl;
				std::exit(1);
			}
			++char_idx;

			if (char_idx > 100000) {
				std::cout << "Error in read json" << std::endl;
				std::exit(1);
			}
		}
		if (line_num % 100000 == 0) {
			std::cout << "Lines read: " << line_num << std::endl;
		}

		// Split by commas not inside double quotes
		std::string doc = "";
		// uint16_t doc_size = 0;
		uint32_t doc_size = 0;
		while (line[char_idx] != '"') {
			if (line[char_idx] == ' ' && doc == "") {
				++char_idx;
				continue;
			}

			if (line[char_idx] == ' ') {
				unique_term_mapping.try_emplace(doc, unique_term_mapping.size());
				terms.push_back(unique_term_mapping[doc]);
				++doc_size;
				doc.clear();
			}
			else {
				doc += toupper(line[char_idx]);
			}
			++char_idx;

			if (char_idx > 10000000) {
				std::cout << "String too large. Code is broken" << std::endl;
				std::exit(1);
			}
		}
		unique_term_mapping.try_emplace(doc, unique_term_mapping.size());
		terms.push_back(unique_term_mapping[doc]);
		++doc_size;
		doc_sizes.push_back(doc_size);
	}
	fclose(file);

	// Write the offsets to a file
	std::ofstream ofs(DIR_NAME + "/" + CSV_LINE_OFFSETS_NAME, std::ios::binary);
	ofs.write((char*)&line_offsets[0], line_offsets.size() * sizeof(uint32_t));
	ofs.close();

	num_docs = doc_sizes.size();
	std::cout << "Million Terms: " << terms.size() / 1000000 << std::endl;
	std::cout << "Num docs: " << num_docs << std::endl;
}

void _BM25::read_csv(std::vector<uint32_t>& terms) {
	// Open the file
	FILE* file = fopen(filename.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << filename << std::endl;
		exit(1);
	}

	int search_column_index = -1;

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint32_t line_num = 0;

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
		line_offsets.push_back(ftell(file) - read);
		++line_num;

		// Iterate of line chars until we get to relevant column.
		int char_idx = 0;
		int col_idx  = 0;
		bool in_quotes = false;
		while (col_idx != search_column_index) {
			if (line[char_idx] == '"' && !in_quotes) {
				in_quotes = !in_quotes;
			}
			else if (line[char_idx] == ',' && !in_quotes) {
				++col_idx;
			}
			++char_idx;
		}

		// Split by commas not inside double quotes
		std::string doc = "";
		in_quotes = false;
		// uint16_t doc_size = 0;
		uint32_t doc_size = 0;
		while (true) {
			if (line[char_idx] == '"' && !in_quotes) {
				in_quotes = true;
			}
			else if (line[char_idx] == '"' && in_quotes) {
				break;
			}
			else if (line[char_idx] == ',' && !in_quotes) {
				break;
			}
			else if (line[char_idx] == '\n' && !in_quotes) {
				break;
			}
			else if (line[char_idx] == ' ') {
				if (unique_term_mapping.find(doc) == unique_term_mapping.end()) {
					unique_term_mapping[doc] = unique_term_mapping.size();
				}
				terms.push_back(unique_term_mapping[doc]);
				++doc_size;
				doc.clear();
			}
			else {
				doc += toupper(line[char_idx]);
			}
			++char_idx;

			if (char_idx > 100000) {
				std::cout << "String too large. Code is broken" << std::endl;
				std::exit(1);
			}
		}
		if (unique_term_mapping.find(doc) == unique_term_mapping.end()) {
			unique_term_mapping[doc] = unique_term_mapping.size();
		}
		terms.push_back(unique_term_mapping[doc]);
		++doc_size;
		doc_sizes.push_back(doc_size);
	}

	fclose(file);

	// Write the offsets to a file
	std::ofstream ofs(DIR_NAME + "/" + CSV_LINE_OFFSETS_NAME, std::ios::binary);
	ofs.write((char*)&line_offsets[0], line_offsets.size() * sizeof(uint32_t));
	ofs.close();

	num_docs = doc_sizes.size();
}

std::vector<std::pair<std::string, std::string>> _BM25::get_csv_line(int line_num) {
	std::ifstream file(filename);
	std::string line;
	file.seekg(line_offsets[line_num]);
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


std::vector<std::pair<std::string, std::string>> _BM25::get_json_line(int line_num) {
	std::ifstream file(filename);
	std::string line;
	file.seekg(line_offsets[line_num]);
	std::getline(file, line);
	file.close();

	// Create effective json by combining column names with values split by commas
	std::vector<std::pair<std::string, std::string>> row;
	bool in_quotes = false;
	size_t col_idx = 0;

	std::string first  = "";
	std::string second = "";

	while (true) {
		while (line[col_idx] != '"') {
			++col_idx;
		}
		++col_idx;

		while (line[col_idx] != '"') {
			first += line[col_idx];
			++col_idx;
		}
		++col_idx;

		while (line[col_idx] != '"') {
			++col_idx;
		}
		++col_idx;

		while (line[col_idx] != '"') {
			second += line[col_idx];
			++col_idx;
		}

		row.emplace_back(first, second);
		first.clear();
		second.clear();

		++col_idx;
		
		if (line[col_idx] == '}') {
			return row;
		}
	}
	return row;
}

void _BM25::save_to_disk() {}

uint32_t _BM25::get_doc_term_freq_db(const uint32_t& term_idx) {
	if (cache_doc_term_freqs) {
		return doc_term_freqs[term_idx];
	}
	return 0;
}

std::vector<uint32_t> _BM25::get_inverted_index_db(const uint32_t& term_idx) {
	if (cache_inverted_index) {
		return inverted_index[term_idx];
	}

	return std::vector<uint32_t>();
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
			fprintf(file, "%s %d\t", std::to_string(pair.first).c_str(), pair.second);
		}

		fprintf(file, "\n");
		offset = ftell(file);
	}
	fclose(file);
}

uint16_t _BM25::get_term_freq_from_file(
		int line_num,
		const uint32_t& term_idx
		) {
	if (cache_term_freqs) {
		float tf = 0.0f;
		for (const std::pair<uint32_t, uint16_t>& term_freq : term_freqs[line_num]) {
			if (term_freq.first == term_idx) {
				tf = term_freq.second;
				break;
			}
		}
		return tf;
	}
	return 0;
}


_BM25::_BM25(
		std::string filename,
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
			filename(filename) {

	auto overall_start = std::chrono::high_resolution_clock::now();
	
	// Read file to get documents, line offsets, and columns
	std::vector<uint32_t> terms;
	if (filename.substr(filename.size() - 3, 3) == "csv") {
		read_csv(terms);
		file_type = CSV;
	}
	else if (filename.substr(filename.size() - 4, 4) == "json") {
		read_json(terms);
		file_type = JSON;
	}
	else {
		std::cout << "Only csv and json files are supported." << std::endl;
		std::exit(1);
	}

	auto read_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> read_elapsed_seconds = read_end - overall_start;
	std::cout << "Read file in " << read_elapsed_seconds.count() << " seconds" << std::endl;

	auto start = std::chrono::high_resolution_clock::now();
	doc_term_freqs.resize(unique_term_mapping.size());

	// Accumulate document frequencies
	uint64_t terms_seen = 0;
	for (size_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		robin_hood::unordered_flat_set<uint32_t> seen_terms;

		// uint16_t doc_size = doc_sizes[doc_id];
		uint32_t doc_size = doc_sizes[doc_id];
		size_t end = terms_seen + doc_size;

		for (size_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint32_t mapped_term_idx = terms[term_idx];

			++doc_term_freqs[mapped_term_idx];

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
	for (size_t term_id = 0; term_id < doc_term_freqs.size(); ++term_id) {
		uint32_t term_count = doc_term_freqs[term_id];
		if (term_count < min_df || term_count > max_number_of_occurrences) {
			blacklisted_terms.insert(term_id);
			doc_term_freqs[term_id] = 0;
		}
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished filtering terms by min_df and max_df in " << elapsed_seconds.count() << "s" << std::endl;
	}

	// Filter terms by min_df and max_df
	inverted_index.resize(unique_term_mapping.size());
	term_freqs.resize(num_docs);

	start = std::chrono::high_resolution_clock::now();
	terms_seen = 0;
	for (uint32_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		// uint16_t doc_size = doc_sizes[doc_id];
		uint32_t doc_size = doc_sizes[doc_id];

		size_t end = terms_seen + doc_size;
		for (size_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint32_t mapped_term_idx = terms[term_idx];
			++terms_seen;

			if (blacklisted_terms.find(mapped_term_idx) != blacklisted_terms.end()) {
				continue;
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
	
	start = std::chrono::high_resolution_clock::now();
	// Write doc_term_freqs to leveldb
	if (!cache_doc_term_freqs) {
		doc_term_freqs.clear();
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished writing doc term frequencies to disk in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();

	// Write inverted index to disk
	if (!cache_inverted_index) {
		inverted_index.clear();
	}

	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;
	if (DEBUG) {
		std::cout << "Finished writing inverted index to disk in " << elapsed_seconds.count() << "s" << std::endl;
	}

	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - overall_start;
	if (DEBUG) {
		std::cout << "Finished creating BM25 index in " << elapsed_seconds.count() << "s" << std::endl;
	}

	avg_doc_size = 0.0f;
	for (int doc_id = 0; doc_id < num_docs; ++doc_id) {
		avg_doc_size += (float)doc_sizes[doc_id];
	}
	avg_doc_size /= num_docs;

	if (DEBUG) {
		// Get mem usage of inverted_index
		uint64_t bytes_used = 0;
		for (const auto& vec : inverted_index) {
			bytes_used += 4 * vec.size();
		}
		std::cout << "Inverted Index MB: " << bytes_used / (1024 * 1024) << std::endl;

		bytes_used = 0;
		for (const auto& vec : term_freqs) {
			bytes_used += 6 * vec.size();
		}
		std::cout << "Term Freqs MB: " << bytes_used / (1024 * 1024) << std::endl;
		std::cout << "Doc Term Freqs MB " << (4 * doc_term_freqs.size()) / (1024 * 1024) << std::endl;
		std::cout << "Doc Sizes MB " << (2 * doc_term_freqs.size()) / (1024 * 1024) << std::endl;
		std::cout << "csv Line Offsets MB " << (4 * line_offsets.size()) / (1024 * 1024) << std::endl;
	}
}

inline float _BM25::_compute_idf(const uint32_t& term_idx) {
	uint32_t df = get_doc_term_freq_db(term_idx);
	return log((num_docs - df + 0.5) / (df + 0.5));
}

inline float _BM25::_compute_bm25(
		const uint32_t& term_idx,
		uint32_t doc_id,
		float tf,
		float idf
		) {
	float doc_size = doc_sizes[doc_id];

	return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_size / avg_doc_size));
}

std::vector<std::pair<uint32_t, float>> _BM25::query(
		std::string& query, 
		uint32_t k,
		uint32_t init_max_df 
		) {
	std::vector<uint32_t> term_idxs;

	std::string substr = "";
	for (const char& c : query) {
		if (c != ' ') {
			substr += c; 
			continue;
		}
		if (unique_term_mapping.find(substr) == unique_term_mapping.end()) {
			continue;
		}
		term_idxs.push_back(unique_term_mapping[substr]);
		substr.clear();
	}
	if (unique_term_mapping.find(substr) != unique_term_mapping.end()) {
		term_idxs.push_back(unique_term_mapping[substr]);
	}

	if (term_idxs.size() == 0) {
		return std::vector<std::pair<uint32_t, float>>();
	}

	// Gather docs that contain at least one term from the query
	// Uses dynamic max_df for performance
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
	std::priority_queue<
		std::pair<uint32_t, float>, 
		std::vector<std::pair<uint32_t, float>>, 
		_compare> top_k_docs;

	// Compute BM25 scores for each candidate doc
	std::vector<float> idfs(term_idxs.size(), 0.0f);
	std::vector<float> tfs(term_idxs.size(), 0.0f);
	int idx = 0;
	for (const uint32_t& doc_id : candidate_docs) {
		float score = 0;
		int   jdx = 0;
		for (const uint32_t& term_idx : term_idxs) {
			if (idx == 0) {
				idfs[jdx] = _compute_idf(term_idx);
				tfs[jdx]  = get_term_freq_from_file(doc_id, term_idx);
			}
			score += _compute_bm25(term_idx, doc_id, tfs[jdx], idfs[jdx]);
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
	for (size_t i = 0; i < top_k_docs.size(); ++i) {
		switch (file_type) {
			case CSV:
				row = get_csv_line(top_k_docs[i].first);
				break;
			case JSON:
				row = get_json_line(top_k_docs[i].first);
				break;
			default:
				std::cout << "Error: Incorrect file type" << std::endl;
				std::exit(1);
				break;
		}
		row.push_back(std::make_pair("score", std::to_string(top_k_docs[i].second)));
		result.push_back(row);
	}
	return result;
}
