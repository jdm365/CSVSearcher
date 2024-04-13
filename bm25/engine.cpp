#include <unistd.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>
#include <stdio.h>

#include <iostream>
#include <vector>
#include <algorithm>
#include <queue>
#include <string>
#include <cstdint>
#include <cmath>
#include <fstream>
#include <sstream>

#include <chrono>
#include <ctime>
#include <sys/mman.h>
#include <fcntl.h>

#include "engine.h"
#include "robin_hood.h"
#include "vbyte_encoding.h"



void update_progress(int line_num, int num_lines) {
	const int bar_width = 121;

	float percentage = (float)line_num / num_lines;
	int   pos = bar_width * percentage;

	// Create the progress bar
	std::string bar;
	if (pos == bar_width) {
		bar = "[" + std::string(bar_width - 1, '=') + ">" + "]";
	}
	else {
		bar = "[" + std::string(pos, '=') + ">" + std::string(bar_width - pos - 1, ' ') + "]";
	}

    // Build the progress string
	// Print percentage instead
	std::string info = std::to_string((int)(percentage * 100)) + "% " +
                           std::to_string(line_num) + " / " + std::to_string(num_lines) + " docs read";
	std::string output =  "\r Indexing Documents " + bar + " " + info;
	output += std::string(std::max(0, bar_width - static_cast<int>(output.length())), ' ');
    
    // Output the progress in one go
    std::cout << output << std::flush;
}


std::vector<uint64_t> get_II_row(
		InvertedIndex* II, 
		uint64_t term_idx
		) {
	std::vector<uint64_t> results_vector;
	decompress_uint64(
			II->inverted_index_compressed[term_idx],
			results_vector
			);

	// Convert doc_ids back to absolute values
	for (size_t i = 2; i < (results_vector.size() + 1) / 2; ++i) {
		results_vector[i] += results_vector[i - 1];
	}
	return results_vector;
}


void _BM25::read_json() {
	// Open the file
	FILE* file = fopen(filename.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << filename << std::endl;
		exit(1);
	}

	// Quickly count number of lines in file
	uint64_t num_lines = 0;
	char buf[1024 * 1024];
	while (size_t bytes_read = fread(buf, 1, sizeof(buf), file)) {
		for (size_t i = 0; i < bytes_read; ++i) {
			if (buf[i] == '\n') {
				++num_lines;
			}
		}
	}
	// Reset file pointer to beginning
	rewind(file);

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;
	uint64_t byte_offset = 0;

	uint64_t unique_terms_found = 0;

	robin_hood::unordered_flat_set<uint64_t> terms_seen;

	std::string doc = "";
	doc.reserve(22);

	const int UPDATE_INTERVAL = 10000;
	while ((read = getline(&line, &len, file)) != -1) {
		if (!DEBUG) {
			if (line_num % UPDATE_INTERVAL == 0) {
				update_progress(line_num, num_lines);
			}
		}

		line_offsets.push_back(byte_offset);
		byte_offset += read;

		terms_seen.clear();

		// Iterate of line chars until we get to relevant column.

		// First char is always `{`
		int char_idx = 1;
		while (true) {
			start:
				while (line[char_idx] == ' ') {
					++char_idx;
				}

				if (line[char_idx] == '"') {
					// Found key. Match against search_col.
					++char_idx;

					for (const char& c : search_col) {
						if (c != line[char_idx]) {
							// Scan until next key
							bool in_quotes = true;
							while (in_quotes) {
								if (line[char_idx] == '"') {
									in_quotes = !in_quotes;
								}
								++char_idx;
							}
							// End of key

							// Scan until comma not in quotes
							while (line[char_idx] != ',') {
								if (line[char_idx] == '"') {
									// Scan to next quote
									while (line[char_idx] != '"') {
										++char_idx;
									}
									while (line[char_idx] == ' ') {
										++char_idx;
									}
									++char_idx;
									continue;
								}
								++char_idx;
							}
							++char_idx;
							goto start;
						}
						++char_idx;
					}

					// Found key. 
					// Iterate over quote and colon.
					char_idx += 2;

					// Check if quote, if so incremenet again then break.
					if (line[char_idx] == '"') ++char_idx;
					break;
				}
				else if (line[char_idx] == '}') {
					std::cout << "Search field not found on line: " << line_num << std::endl;
					std::exit(1);
				}
				else {
					std::cerr << "Invalid json." << std::endl;
					std::cout << line << std::endl;
					std::exit(1);
				}
		}
		while (line[char_idx] == ' ') {
			++char_idx;
		}
		++char_idx;

		std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> pq;
		pq.push(line_num);

		// Split by commas not inside double quotes
		uint64_t doc_size = 0;
		while (line[char_idx] != '"') {
			if (line[char_idx] == '\\') {
				++char_idx;
				doc += line[char_idx];
				++char_idx;
				continue;
			}

			if (line[char_idx] == ' ' && doc == "") {
				++char_idx;
				continue;
			}

			if (line[char_idx] == ' ') {
				auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);
				if (add) {
					// New term
					II.accumulator.push_back(pq);

					terms_seen.insert(it->second);

					++unique_terms_found;
				}
				else {
					// Term already exists
					if (terms_seen.find(it->second) == terms_seen.end()) {
						terms_seen.insert(it->second);
						II.accumulator[it->second].push(line_num);
					}
				}

				++doc_size;
				doc.clear();

				++char_idx;
				continue;
			}

			doc += toupper(line[char_idx]);
			++char_idx;
		}

		auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);

		if (add) {
			// New term
			II.accumulator.push_back(pq);

			++unique_terms_found;
		}
		else {
			// Term already exists
			if (terms_seen.find(it->second) == terms_seen.end()) {
				II.accumulator[it->second].push(line_num);
			}
		}

		++doc_size;
		doc_sizes.push_back(doc_size);
		doc.clear();
		++line_num;
	}
	fclose(file);

	update_progress(line_num, num_lines);

	std::cout << std::endl << std::flush;
	std::cout << "Compressing index" << std::endl;

	if (DEBUG) {
		std::cout << "Vocab size: " << unique_terms_found << std::endl;
	}

	num_docs = num_lines;
	// Calc avg_doc_size
	float sum = 0;
	for (const auto& size : doc_sizes) {
		sum += size;
	}
	avg_doc_size = sum / num_docs;

	auto start = std::chrono::system_clock::now();

	// Now get inverted_index_compressed from accumulator.
	II.inverted_index_compressed.resize(unique_terms_found);
	uint64_t idx = 0;
	for (auto& row : II.accumulator) {
		std::vector<uint64_t> uncompressed_buffer(1, 0);
		uncompressed_buffer.reserve(row.size() * 2 + 1);
		std::vector<uint64_t> tfs;

		uint64_t last_doc_id = UINT64_MAX;
		uint64_t same_count = 1;
		bool first = true;
		while (!row.empty()) {
			auto doc_id = row.top();

			if (doc_id == last_doc_id) {
				++same_count;
			}
			else {
				++uncompressed_buffer[0];

				// Make doc ids differential
				uncompressed_buffer.push_back(doc_id - !first * last_doc_id);
				tfs.push_back(same_count);

				same_count = 1;
			}

			// remove top element
			row.pop();
			last_doc_id = doc_id;

			first = false;
		}
		// Add tfs to end of uncompressed_buffer
		for (const auto& tf : tfs) {
			uncompressed_buffer.push_back(tf);
		}

		II.inverted_index_compressed[idx].reserve(2 * uncompressed_buffer.size());
		compress_uint64(
				uncompressed_buffer,
				II.inverted_index_compressed[idx]
				);
		++idx;
	}

	auto end = std::chrono::system_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;
	std::cout << "Time to compress: " << elapsed_seconds.count() << std::endl;

	II.accumulator.clear();
	II.accumulator.shrink_to_fit();

	if (DEBUG) {
		uint64_t total_size = 0;
		for (const auto& row : II.inverted_index_compressed) {
			total_size += row.size();
		}
		total_size /= 1024 * 1024;
		std::cout << "Total size of inverted index: " << total_size << "MB" << std::endl;
	}
}



void _BM25::read_csv() {
	// Open the file
	FILE* file = fopen(filename.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << filename << std::endl;
		exit(1);
	}

	// Quickly count number of lines in file
	uint64_t num_lines = 0;
	char buf[1024 * 1024];
	while (size_t bytes_read = fread(buf, 1, sizeof(buf), file)) {
		for (size_t i = 0; i < bytes_read; ++i) {
			if (buf[i] == '\n') {
				++num_lines;
			}
		}
	}
	// Reset file pointer to beginning
	rewind(file);

	int search_column_index = -1;

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;
	uint64_t byte_offset = 0;

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
		std::cerr << "Cols found:  ";
		for (size_t i = 0; i < columns.size(); ++i) {
			std::cerr << columns[i] << ",";
		}
		std::cerr << std::endl;
		exit(1);
	}
	byte_offset += ftell(file);

	uint64_t unique_terms_found = 0;

	// Small string optimization limit on most platforms
	std::string doc = "";
	doc.reserve(22);

	robin_hood::unordered_flat_set<uint64_t> terms_seen;

	const int UPDATE_INTERVAL = 10000;
	while ((read = getline(&line, &len, file)) != -1) {
		if (line_num % UPDATE_INTERVAL == 0) {
			update_progress(line_num, num_lines);
		}
		line_offsets.push_back(byte_offset);
		byte_offset += read;

		terms_seen.clear();

		// Iterate of line chars until we get to relevant column.
		int char_idx = 0;
		int col_idx  = 0;
		while (col_idx != search_column_index) {
			if (line[char_idx] == '"') {
				// Skip to next quote.
				++char_idx;
				while (line[char_idx] == '"') {
					++char_idx;
				}
			}

			if (line[char_idx] == ',') {
				++col_idx;
			}
			++char_idx;
		}

		// Split by commas not inside double quotes
		uint64_t doc_size = 0;
		char end_delim = ',';
		if (search_column_index == (int)columns.size() - 1) {
			end_delim = '\n';
		}
		if (line[char_idx] == '"') {
			end_delim = '"';
			++char_idx;
		}

		std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> pq;
		pq.push(line_num);

		while (line[char_idx] != end_delim) {
			if (line[char_idx] == ' ') {
				auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);
				if (add) {
					// New term
					II.accumulator.push_back(pq);

					terms_seen.insert(it->second);

					++unique_terms_found;
				}
				else {
					// Term already exists
					if (terms_seen.find(it->second) == terms_seen.end()) {
						terms_seen.insert(it->second);
						II.accumulator[it->second].push(line_num);
					}
				}

				++doc_size;
				doc.clear();

				++char_idx;
				continue;
			}

			doc += toupper(line[char_idx]);
			++char_idx;
		}

		auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);

		if (add) {
			// New term
			II.accumulator.push_back(pq);

			++unique_terms_found;
		}
		else {
			// Term already exists
			if (terms_seen.find(it->second) == terms_seen.end()) {
				II.accumulator[it->second].push(line_num);
			}
		}

		++line_num;
		++doc_size;
		doc_sizes.push_back(doc_size);
		doc.clear();
	}
	fclose(file);
	update_progress(line_num + 1, num_lines);

	std::cout << std::endl << std::flush;
	std::cout << "Compressing index" << std::endl;

	if (DEBUG) {
		std::cout << "Vocab size: " << unique_terms_found << std::endl;
	}

	num_docs = doc_sizes.size();
	// Calc avg_doc_size
	float sum = 0;
	for (const auto& size : doc_sizes) {
		sum += size;
	}
	avg_doc_size = sum / num_docs;

	auto start = std::chrono::system_clock::now();

	// Now get inverted_index_compressed from accumulator.
	II.inverted_index_compressed.resize(unique_terms_found);
	uint64_t idx = 0;
	for (auto& row : II.accumulator) {
		std::vector<uint64_t> uncompressed_buffer(1, 0);
		uncompressed_buffer.reserve(row.size() * 2 + 1);
		std::vector<uint64_t> tfs;

		uint64_t last_doc_id = UINT64_MAX;
		uint64_t same_count = 1;
		bool first = true;
		while (!row.empty()) {
			auto doc_id = row.top();

			if (doc_id == last_doc_id) {
				++same_count;
			}
			else {
				++uncompressed_buffer[0];

				// Make doc ids differential
				uncompressed_buffer.push_back(doc_id - !first * last_doc_id);
				tfs.push_back(same_count);

				same_count = 1;
			}

			// remove top element
			row.pop();
			last_doc_id = doc_id;

			first = false;
		}
		// Add tfs to end of uncompressed_buffer
		for (const auto& tf : tfs) {
			uncompressed_buffer.push_back(tf);
		}

		II.inverted_index_compressed[idx].reserve(2 * uncompressed_buffer.size());
		compress_uint64(
				uncompressed_buffer,
				II.inverted_index_compressed[idx]
				);
		++idx;
	}

	auto end = std::chrono::system_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;
	std::cout << "Time to compress: " << elapsed_seconds.count() << std::endl;

	II.accumulator.clear();
	II.accumulator.shrink_to_fit();

	if (DEBUG) {
		uint64_t total_size = 0;
		for (const auto& row : II.inverted_index_compressed) {
			total_size += row.size();
		}
		total_size /= 1024 * 1024;
		std::cout << "Total size of inverted index: " << total_size << "MB" << std::endl;
	}
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

	std::string first  = "";
	std::string second = "";

	if (line[1] == '}') {
		return row;
	}

	size_t char_idx = 2;
	while (true) {
		while (line[char_idx] != '"') {
			if (line[char_idx] == '\\') {
				++char_idx;
				first += line[char_idx];
				++char_idx;
				continue;
			}
			first += line[char_idx];
			++char_idx;
		}
		char_idx += 2;

		if (line[char_idx] == '"') {
			++char_idx;

			while (line[char_idx] != '"') {
				if (line[char_idx] == '\\') {
					++char_idx;
					first += line[char_idx];
					++char_idx;
					continue;
				}

				second += line[char_idx];
				++char_idx;
			}
			++char_idx;

			if (line[char_idx] == '}') {
				row.emplace_back(first, second);
				return row;
			}

			row.emplace_back(first, second);
			first.clear();
			second.clear();
			++char_idx;
		}
		else {
			while (line[char_idx] != ',') {
				if (line[char_idx] == '}') {
					row.emplace_back(first, second);
					return row;
				}
				if (line[char_idx] == '\\') {
					++char_idx;
					first += line[char_idx];
					++char_idx;
					continue;
				}

				second += line[char_idx];
				++char_idx;
			}
			char_idx += 2;

			row.emplace_back(first, second);
			first.clear();
			second.clear();
		}
		if (line[char_idx] != '"') {
			std::cout << line << std::endl;
			std::cout << char_idx << std::endl << std::endl;
		}
		++char_idx;
	}
	return row;
}

void serialize_vector_u8(const std::vector<uint8_t>& vec, const std::string& filename) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file when serializing u8 vector.\n";
        return;
    }

    // Write the size of the vector first
    size_t size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

    // Write the vector elements
	for (const auto& val : vec) {
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(uint8_t));
	}

    out_file.close();
}

void serialize_vector_u32(const std::vector<uint32_t>& vec, const std::string& filename) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file when serializing u32 vector.\n";
        return;
    }

    // Write the size of the vector first
    size_t size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

    // Write the vector elements
	for (const auto& val : vec) {
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(uint32_t));
	}

    out_file.close();
}

void serialize_vector_u64(const std::vector<uint64_t>& vec, const std::string& filename) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file when serializing u64 vector.\n";
        return;
    }

    // Write the size of the vector first
    size_t size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

    // Write the vector elements
	for (const auto& val : vec) {
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(uint64_t));
	}

    out_file.close();
}

void serialize_vector_of_vectors_u8(
		const std::vector<std::vector<uint8_t>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector first
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    for (const auto& inner_vec : vec) {
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the elements of the inner vector
        if (!inner_vec.empty()) {
            out_file.write(
					reinterpret_cast<const char*>(inner_vec.data()), 
					inner_vec.size() * sizeof(uint8_t)
					);
        }
    }

    out_file.close();
}

void serialize_vector_of_vectors_u32(
		const std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector first
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    for (const auto& inner_vec : vec) {
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the elements of the inner vector
        if (!inner_vec.empty()) {
            out_file.write(reinterpret_cast<const char*>(inner_vec.data()), inner_vec.size() * sizeof(uint32_t));
        }
    }

    out_file.close();
}

void serialize_vector_of_vectors_u64(
		const std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector first
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    for (const auto& inner_vec : vec) {
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the elements of the inner vector
        if (!inner_vec.empty()) {
            out_file.write(reinterpret_cast<const char*>(inner_vec.data()), inner_vec.size() * sizeof(uint64_t));
        }
    }

    out_file.close();
}

void serialize_robin_hood_flat_map_string_u32(
		const robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the map first
    size_t map_size = map.size();
    out_file.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

    for (const auto& [key, value] : map) {
        size_t key_size = key.size();
        out_file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out_file.write(key.data(), key_size);
        out_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    out_file.close();
}

void serialize_robin_hood_flat_map_string_u64(
		const robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the map first
    size_t map_size = map.size();
    out_file.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

    for (const auto& [key, value] : map) {
        size_t key_size = key.size();
        out_file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out_file.write(key.data(), key_size);
        out_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    out_file.close();
}

void serialize_vector_of_vectors_pair_u32_u16(
		const std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    // Iterate through the outer vector
    for (const auto& inner_vec : vec) {
        // Write the size of the inner vector
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the pairs
        for (const auto& [first, second] : inner_vec) {
            out_file.write(reinterpret_cast<const char*>(&first), sizeof(first));
            out_file.write(reinterpret_cast<const char*>(&second), sizeof(second));
        }
    }

    out_file.close();
}

void serialize_vector_of_vectors_pair_u64_u16(
		const std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    // Iterate through the outer vector
    for (const auto& inner_vec : vec) {
        // Write the size of the inner vector
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the pairs
        for (const auto& [first, second] : inner_vec) {
            out_file.write(reinterpret_cast<const char*>(&first), sizeof(first));
            out_file.write(reinterpret_cast<const char*>(&second), sizeof(second));
        }
    }

    out_file.close();
}

void deserialize_vector_u8(std::vector<uint8_t>& vec, const std::string& filename) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the vector
    size_t size;
    in_file.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Resize the vector and read its elements
    vec.resize(size);
    if (size > 0) {
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(uint8_t));
    }

    in_file.close();
}

void deserialize_vector_u32(std::vector<uint32_t>& vec, const std::string& filename) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the vector
    size_t size;
    in_file.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Resize the vector and read its elements
    vec.resize(size);
    if (size > 0) {
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(uint32_t));
    }

    in_file.close();
}

void deserialize_vector_u64(std::vector<uint64_t>& vec, const std::string& filename) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the vector
    size_t size;
    in_file.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Resize the vector and read its elements
    vec.resize(size);
    if (size > 0) {
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(uint64_t));
    }

    in_file.close();
}

void deserialize_vector_of_vectors_u8(
		std::vector<std::vector<uint8_t>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	if (vec.size() > 0) {
		vec.clear();
	}

	// Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    // Iterate through the outer vector
    for (auto& inner_vec : vec) {
        // Read the size of the inner vector
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the elements of the inner vector
        if (inner_size > 0) {
            in_file.read(reinterpret_cast<char*>(inner_vec.data()), inner_size * sizeof(uint8_t));
        }
    }

    in_file.close();
}

void deserialize_vector_of_vectors_u32(
		std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	if (vec.size() > 0) {
		vec.clear();
	}

	// Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    // Iterate through the outer vector
    for (auto& inner_vec : vec) {
        // Read the size of the inner vector
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the elements of the inner vector
        if (inner_size > 0) {
            in_file.read(reinterpret_cast<char*>(inner_vec.data()), inner_size * sizeof(uint32_t));
        }
    }

    in_file.close();
}


void deserialize_vector_of_vectors_u64(
		std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	if (vec.size() > 0) {
		vec.clear();
	}

	// Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    // Iterate through the outer vector
    for (auto& inner_vec : vec) {
        // Read the size of the inner vector
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the elements of the inner vector
        if (inner_size > 0) {
            in_file.read(reinterpret_cast<char*>(inner_vec.data()), inner_size * sizeof(uint64_t));
        }
    }

    in_file.close();
}

void deserialize_robin_hood_flat_map_string_u32(
		robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	// Read the size of the map
    size_t map_size;
    in_file.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

    // Read each key-value pair and insert it into the map
    for (size_t i = 0; i < map_size; ++i) {
        size_t keySize;
        in_file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
        
        std::string key(keySize, '\0');
        in_file.read(&key[0], keySize);

        uint32_t value;
        in_file.read(reinterpret_cast<char*>(&value), sizeof(value));

        map[key] = value;
    }

    in_file.close();
}

void deserialize_robin_hood_flat_map_string_u64(
		robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	// Read the size of the map
    size_t map_size;
    in_file.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

    // Read each key-value pair and insert it into the map
    for (size_t i = 0; i < map_size; ++i) {
        size_t keySize;
        in_file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
        
        std::string key(keySize, '\0');
        in_file.read(&key[0], keySize);

        uint64_t value;
        in_file.read(reinterpret_cast<char*>(&value), sizeof(value));

        map[key] = value;
    }

    in_file.close();
}

void deserialize_vector_of_vectors_pair_u32_u16(
		std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    for (auto& inner_vec : vec) {
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the pairs
        for (auto& [first, second] : inner_vec) {
            in_file.read(reinterpret_cast<char*>(&first), sizeof(first));
            in_file.read(reinterpret_cast<char*>(&second), sizeof(second));
        }
    }

    in_file.close();
}

void deserialize_vector_of_vectors_pair_u64_u16(
		std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    for (auto& inner_vec : vec) {
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the pairs
        for (auto& [first, second] : inner_vec) {
            in_file.read(reinterpret_cast<char*>(&first), sizeof(first));
            in_file.read(reinterpret_cast<char*>(&second), sizeof(second));
        }
    }

    in_file.close();
}

void _BM25::save_to_disk(const std::string& db_dir) {
	auto start = std::chrono::high_resolution_clock::now();

	if (access(db_dir.c_str(), F_OK) != -1) {
		// Remove the directory if it exists
		std::string command = "rm -r " + db_dir;
		system(command.c_str());

		// Create the directory
		command = "mkdir " + db_dir;
		system(command.c_str());
	}
	else {
		// Create the directory if it does not exist
		std::string command = "mkdir " + db_dir;
		system(command.c_str());
	}

	// Join paths
	std::string UNIQUE_TERM_MAPPING_PATH = db_dir + "/unique_term_mapping.bin";
	std::string INVERTED_INDEX_PATH 	 = db_dir + "/inverted_index.bin";
	std::string DOC_SIZES_PATH 		     = db_dir + "/doc_sizes.bin";
	std::string LINE_OFFSETS_PATH 		 = db_dir + "/line_offsets.bin";
	std::string METADATA_PATH 			 = db_dir + "/metadata.bin";

	serialize_robin_hood_flat_map_string_u64(unique_term_mapping, UNIQUE_TERM_MAPPING_PATH);
	serialize_vector_of_vectors_u8(II.inverted_index_compressed, INVERTED_INDEX_PATH);
	// serialize_vector_u64(doc_sizes, DOC_SIZES_PATH);
	// serialize_vector_u64(line_offsets, LINE_OFFSETS_PATH);
	std::vector<uint8_t> compressed_doc_sizes;
	std::vector<uint8_t> compressed_line_offsets;
	compressed_doc_sizes.reserve(doc_sizes.size() * 2);
	compressed_line_offsets.reserve(line_offsets.size() * 2);
	compress_uint64(doc_sizes, compressed_doc_sizes);
	compress_uint64(line_offsets, compressed_line_offsets);
	serialize_vector_u8(compressed_doc_sizes, DOC_SIZES_PATH);
	serialize_vector_u8(compressed_line_offsets, LINE_OFFSETS_PATH);

	// Serialize smaller members.
	std::ofstream out_file(METADATA_PATH, std::ios::binary);
	if (!out_file) {
		std::cerr << "Error opening file for writing.\n";
		return;
	}

	// Write basic types directly
	out_file.write(reinterpret_cast<const char*>(&num_docs), sizeof(num_docs));
	out_file.write(reinterpret_cast<const char*>(&min_df), sizeof(min_df));
	out_file.write(reinterpret_cast<const char*>(&avg_doc_size), sizeof(avg_doc_size));
	out_file.write(reinterpret_cast<const char*>(&max_df), sizeof(max_df));
	out_file.write(reinterpret_cast<const char*>(&k1), sizeof(k1));
	out_file.write(reinterpret_cast<const char*>(&b), sizeof(b));

	// Write enum as int
	int file_type_int = static_cast<int>(file_type);
	out_file.write(reinterpret_cast<const char*>(&file_type_int), sizeof(file_type_int));

	// Write std::string
	size_t filename_length = filename.size();
	out_file.write(reinterpret_cast<const char*>(&filename_length), sizeof(filename_length));
	out_file.write(filename.data(), filename_length);

	// Write std::vector<std::string>
	size_t columns_size = columns.size();
	out_file.write(reinterpret_cast<const char*>(&columns_size), sizeof(columns_size));
	for (const auto& col : columns) {
		size_t col_length = col.size();
		out_file.write(reinterpret_cast<const char*>(&col_length), sizeof(col_length));
		out_file.write(col.data(), col_length);
	}

	// Write search_col std::string
	size_t search_col_length = search_col.size();
	out_file.write(reinterpret_cast<const char*>(&search_col_length), sizeof(search_col_length));
	out_file.write(search_col.data(), search_col_length);

	out_file.close();

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Saved in " << elapsed_seconds.count() << "s" << std::endl;
	}
}

void _BM25::load_from_disk(const std::string& db_dir) {
	auto start = std::chrono::high_resolution_clock::now();

	// Join paths
	std::string UNIQUE_TERM_MAPPING_PATH = db_dir + "/unique_term_mapping.bin";
	std::string INVERTED_INDEX_PATH 	 = db_dir + "/inverted_index.bin";
	std::string DOC_SIZES_PATH 		     = db_dir + "/doc_sizes.bin";
	std::string LINE_OFFSETS_PATH 		 = db_dir + "/line_offsets.bin";
	std::string METADATA_PATH 			 = db_dir + "/metadata.bin";

	deserialize_robin_hood_flat_map_string_u64(unique_term_mapping, UNIQUE_TERM_MAPPING_PATH);
	deserialize_vector_of_vectors_u8(II.inverted_index_compressed, INVERTED_INDEX_PATH);
	// deserialize_vector_u64(doc_sizes, DOC_SIZES_PATH);
	// deserialize_vector_u64(line_offsets, LINE_OFFSETS_PATH);
	std::vector<uint8_t> compressed_doc_sizes;
	std::vector<uint8_t> compressed_line_offsets;
	deserialize_vector_u8(compressed_doc_sizes, DOC_SIZES_PATH);
	deserialize_vector_u8(compressed_line_offsets, LINE_OFFSETS_PATH);
	decompress_uint64(compressed_doc_sizes, doc_sizes);
	decompress_uint64(compressed_line_offsets, line_offsets);

	// Load smaller members.
	std::ifstream in_file(METADATA_PATH, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read basic types directly
    in_file.read(reinterpret_cast<char*>(&num_docs), sizeof(num_docs));
    in_file.read(reinterpret_cast<char*>(&min_df), sizeof(min_df));
    in_file.read(reinterpret_cast<char*>(&avg_doc_size), sizeof(avg_doc_size));
    in_file.read(reinterpret_cast<char*>(&max_df), sizeof(max_df));
    in_file.read(reinterpret_cast<char*>(&k1), sizeof(k1));
    in_file.read(reinterpret_cast<char*>(&b), sizeof(b));

    // Read enum as int
    int file_type_int;
    in_file.read(reinterpret_cast<char*>(&file_type_int), sizeof(file_type_int));
    file_type = static_cast<SupportedFileTypes>(file_type_int);

    // Read std::string
    size_t filename_length;
    in_file.read(reinterpret_cast<char*>(&filename_length), sizeof(filename_length));
    filename.resize(filename_length);
    in_file.read(&filename[0], filename_length);

    // Read std::vector<std::string>
    size_t columns_size;
    in_file.read(reinterpret_cast<char*>(&columns_size), sizeof(columns_size));
    columns.resize(columns_size);
    for (auto& col : columns) {
        size_t col_length;
        in_file.read(reinterpret_cast<char*>(&col_length), sizeof(col_length));
        col.resize(col_length);
        in_file.read(&col[0], col_length);
    }

    // Read search_col std::string
    size_t search_col_length;
    in_file.read(reinterpret_cast<char*>(&search_col_length), sizeof(search_col_length));
    search_col.resize(search_col_length);
    in_file.read(&search_col[0], search_col_length);

    in_file.close();

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Loaded in " << elapsed_seconds.count() << "s" << std::endl;
	}
}


_BM25::_BM25(
		std::string filename,
		std::string search_col,
		int min_df,
		float max_df,
		float k1,
		float b
		) : min_df(min_df), 
			max_df(max_df), 
			k1(k1), 
			b(b),
			search_col(search_col), 
			filename(filename) {

	auto overall_start = std::chrono::high_resolution_clock::now();
	
	// Read file to get documents, line offsets, and columns
	if (filename.substr(filename.size() - 3, 3) == "csv") {
		read_csv();
		file_type = CSV;
	}
	else if (filename.substr(filename.size() - 4, 4) == "json") {
		read_json();
		file_type = JSON;
	}
	else {
		std::cout << "Only csv and json files are supported." << std::endl;
		std::exit(1);
	}

	if (max_df <= 1.0) {
		max_df = (int)num_docs * max_df;
	}

	if (DEBUG) {
		auto read_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> read_elapsed_seconds = read_end - overall_start;
		std::cout << "Read file in " << read_elapsed_seconds.count() << " seconds" << std::endl;
	}
}


_BM25::_BM25(
		std::vector<std::string>& documents,
		int min_df,
		float max_df,
		float k1,
		float b
		) : min_df(min_df), 
			max_df(max_df), 
			k1(k1), 
			b(b) {
	filename = "in_memory";
	file_type = IN_MEMORY;

	num_docs = documents.size();

	uint64_t unique_terms_found = 0;

	std::string doc = "";
	doc.reserve(22);

	robin_hood::unordered_flat_set<uint64_t> terms_seen;
	const int UPDATE_INTERVAL = 10000;
	uint64_t doc_id = 0;
	for (const std::string& line : documents) {
		terms_seen.clear();

		if (doc_id % UPDATE_INTERVAL == 0) {
			// Clear existing line and print progress
			update_progress(doc_id, num_docs);
		}

		// Split by commas not inside double quotes
		uint32_t doc_size = 0;

		std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> pq;
		pq.push(doc_id);

		for (const char& c : line) {
			if (c == ' ') {
				auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);
				if (add) {
					// New term
					II.accumulator.push_back(pq);

					terms_seen.insert(it->second);

					++unique_terms_found;
				}
				else {
					// Term already exists
					if (terms_seen.find(it->second) == terms_seen.end()) {
						terms_seen.insert(it->second);
						II.accumulator[it->second].push(doc_id);
					}
				}

				++doc_size;
				doc.clear();
				continue;
			}

			doc += toupper(c);
		}

		auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);

		if (add) {
			// New term
			II.accumulator.push_back(pq);

			++unique_terms_found;
		}
		else {
			// Term already exists
			if (terms_seen.find(it->second) == terms_seen.end()) {
				II.accumulator[it->second].push(doc_id);
			}
		}

		doc_sizes.push_back(doc_size);
		doc.clear();
		++doc_id;
	}
	update_progress(doc_id, num_docs);

	avg_doc_size = 0.0f;
	for (const uint64_t& size : doc_sizes) {
		avg_doc_size += size;
	}
	avg_doc_size /= num_docs;


	std::cout << std::endl << std::flush;
	std::cout << "Compressing index" << std::endl;

	if (DEBUG) {
		std::cout << "Vocab size: " << unique_terms_found << std::endl;
	}

	// Calc avg_doc_size
	float sum = 0;
	for (const auto& size : doc_sizes) {
		sum += size;
	}
	avg_doc_size = sum / num_docs;

	auto start = std::chrono::system_clock::now();

	// Now get inverted_index_compressed from accumulator.
	II.inverted_index_compressed.resize(unique_terms_found);
	uint64_t idx = 0;
	for (auto& row : II.accumulator) {
		std::vector<uint64_t> uncompressed_buffer(1, 0);
		uncompressed_buffer.reserve(row.size() * 2 + 1);
		std::vector<uint64_t> tfs;

		uint64_t last_doc_id = UINT64_MAX;
		uint64_t same_count = 1;
		bool first = true;
		while (!row.empty()) {
			auto doc_id = row.top();

			if (doc_id == last_doc_id) {
				++same_count;
			}
			else {
				++uncompressed_buffer[0];

				// Make doc ids differential
				uncompressed_buffer.push_back(doc_id - !first * last_doc_id);
				tfs.push_back(same_count);

				same_count = 1;
			}

			// remove top element
			row.pop();
			last_doc_id = doc_id;

			first = false;
		}
		// Add tfs to end of uncompressed_buffer
		for (const auto& tf : tfs) {
			uncompressed_buffer.push_back(tf);
		}

		II.inverted_index_compressed[idx].reserve(2 * uncompressed_buffer.size());
		compress_uint64(
				uncompressed_buffer,
				II.inverted_index_compressed[idx]
				);
		++idx;
	}

	auto end = std::chrono::system_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;
	std::cout << "Time to compress: " << elapsed_seconds.count() << std::endl;

	II.accumulator.clear();
	II.accumulator.shrink_to_fit();

	if (DEBUG) {
		uint64_t total_size = 0;
		for (const auto& row : II.inverted_index_compressed) {
			total_size += row.size();
		}
		total_size /= 1024 * 1024;
		std::cout << "Total size of inverted index: " << total_size << "MB" << std::endl;
	}
}

inline float _BM25::_compute_bm25(
		uint64_t doc_id,
		float tf,
		float idf
		) {
	float doc_size = doc_sizes[doc_id];

	return idf * tf / (tf + k1 * (1 - b + b * doc_size / avg_doc_size));
}

std::vector<std::pair<uint64_t, float>> _BM25::query(
		std::string& query, 
		uint32_t k,
		uint32_t init_max_df 
		) {
	std::vector<uint64_t> term_idxs;

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
		return std::vector<std::pair<uint64_t, float>>();
	}

	// Gather docs that contain at least one term from the query
	// Uses dynamic max_df for performance
	uint64_t local_max_df = init_max_df;
	robin_hood::unordered_map<uint64_t, float> doc_scores;

	auto start = std::chrono::high_resolution_clock::now();
	while (doc_scores.size() == 0) {
		for (const uint64_t& term_idx : term_idxs) {
			uint64_t df;
			vbyte_decode_uint64(
					II.inverted_index_compressed[term_idx].data(),
					&df
					);
			if (df > local_max_df) {
				continue;
			}
			float idf = log((num_docs - df + 0.5) / (df + 0.5));

			std::vector<uint64_t> results_vector = get_II_row(&II, term_idx);
			uint64_t num_matches = (results_vector.size() - 1) / 2;

			if (num_matches == 0) {
				continue;
			}

			for (size_t i = 0; i < num_matches; ++i) {
				uint64_t doc_id  = results_vector[i + 1];
				float tf 		 = (float)results_vector[i + num_matches + 1];
				float bm25_score = _compute_bm25(doc_id, tf, idf);

				if (doc_scores.find(doc_id) == doc_scores.end()) {
					doc_scores[doc_id] = bm25_score;
				}
				else {
					doc_scores[doc_id] += bm25_score;
				}
			}

		}
		local_max_df *= 10;

		if (local_max_df > num_docs || local_max_df > max_df) {
			break;
		}
	}
	if (DEBUG) {
		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> elapsed_seconds = end - start;
		std::cout << "Collection time: " << elapsed_seconds.count() << std::endl;
	}
	
	if (doc_scores.size() == 0) {
		return std::vector<std::pair<uint64_t, float>>();
	}

	std::priority_queue<
		std::pair<uint64_t, float>, 
		std::vector<std::pair<uint64_t, float>>, 
		_compare_64> top_k_docs;

	for (const auto& pair : doc_scores) {
		top_k_docs.push(std::make_pair(pair.first, pair.second));
		if (top_k_docs.size() > k) {
			top_k_docs.pop();
		}
	}

	std::vector<std::pair<uint64_t, float>> result(top_k_docs.size());
	int idx = top_k_docs.size() - 1;
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
	std::vector<std::pair<uint64_t, float>> top_k_docs = query(_query, top_k, init_max_df);
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
			case IN_MEMORY:
				std::cout << "Error: In-memory data not supported for this function." << std::endl;
				std::exit(1);
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
