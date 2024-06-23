#include <unistd.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
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
#include <omp.h>
#include <thread>
#include <mutex>
#include <termios.h>

#include "engine.h"
#include "robin_hood.h"
#include "vbyte_encoding.h"
#include "serialize.h"
#include "bloom/filter.hpp"

static inline void get_optimal_params(
		uint64_t num_docs,
		double fpr,
		uint64_t &num_hashes,
		uint64_t &num_bits
		) {
	const double optimal_hash_count = -log2(fpr);
	num_hashes = round(optimal_hash_count);
	num_bits   = ceil((num_docs * log(fpr)) / log(1 / pow(2, log(2))));
}

BloomEntry init_bloom_entry(uint32_t num_docs, double fpr) {
	uint64_t num_hashes, num_bits;
	get_optimal_params(num_docs, fpr, num_hashes, num_bits);
	Bloom::Filter bf(num_bits, num_hashes);

	if (DEBUG) {
		printf("Num docs: %u\n", num_docs);
		printf("Num hashes: %lu\n", num_hashes);
		printf("Num bits: %lu\n", num_bits);
		printf("FPR: %f\n\n", fpr);
		fflush(stdout);
	}

	BloomEntry bloom_entry = {
		.bloom_filter = bf,
		.topk_doc_ids = std::vector<uint64_t>()
	};
	return bloom_entry;
}

static inline bool is_valid_token(std::string& str) {
	return (str.size() > 1 || isalnum(str[0]));
}

bool output_is_terminal() {
	return isatty(fileno(stdout));
}

void set_raw_mode() {
    termios term;
    tcgetattr(STDIN_FILENO, &term);
    term.c_lflag &= ~(ICANON | ECHO); // Disable echo and canonical mode
    tcsetattr(STDIN_FILENO, TCSANOW, &term);
}

// Function to reset the terminal to normal mode
void reset_terminal_mode() {
    termios term;
    tcgetattr(STDIN_FILENO, &term);
    term.c_lflag |= (ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &term);
}

// Function to query the cursor position
void get_cursor_position(int& rows, int& cols) {
    set_raw_mode();

    // Send the ANSI code to report cursor position
    std::cout << "\x1b[6n" << std::flush;

    // Expecting response in the format: ESC[row;colR
    char ch;
    int rows_temp = 0, cols_temp = 0;
    int read_state = 0;

    while (std::cin.get(ch)) {
        if (ch == '\x1b') {
            read_state = 1;
        } else if (ch == '[' && read_state == 1) {
            read_state = 2;
        } else if (ch == 'R') {
            break;
        } else if (read_state == 2 && ch != ';') {
            rows_temp = rows_temp * 10 + (ch - '0');
        } else if (ch == ';') {
            read_state = 3;
        } else if (read_state == 3) {
            cols_temp = cols_temp * 10 + (ch - '0');
        }
    }

    reset_terminal_mode();

    rows = rows_temp;
    cols = cols_temp;
}

void get_terminal_size(int& rows, int& cols) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == -1) {
        perror("ioctl");
        exit(EXIT_FAILURE);
    }
    rows = ws.ws_row;
    cols = ws.ws_col;
}


void _BM25::determine_partition_boundaries_csv() {
	// First find number of bytes in file.
	// Get avg chunk size in bytes.
	// Seek in jumps of byte chunks, then scan forward to newline and append to partition_boundaries.
	// If we reach end of file, break.

	FILE* f = reference_file_handles[0];

	struct stat sb;
	if (fstat(fileno(f), &sb) == -1) {
		std::cerr << "Error getting file size." << std::endl;
		std::exit(1);
	}

	size_t file_size = sb.st_size;
	size_t chunk_size = file_size / num_partitions;

	partition_boundaries.push_back(header_bytes);

	size_t byte_offset = header_bytes;
	while (true) {
		byte_offset += chunk_size;

		if (byte_offset >= file_size) {
			partition_boundaries.push_back(file_size);
			break;
		}

		fseek(f, byte_offset, SEEK_SET);

		char buf[1024];
		while (true) {
			size_t bytes_read = fread(buf, 1, sizeof(buf), f);
			for (size_t i = 0; i < bytes_read; ++i) {
				if (buf[i] == '\n') {
					partition_boundaries.push_back(++byte_offset);
					goto end_of_loop;
				}
				++byte_offset;
			}
		}

		end_of_loop:
			continue;
	}

	if (partition_boundaries.size() != num_partitions + 1) {
		printf("Partition boundaries: %lu\n", partition_boundaries.size());
		printf("Num partitions: %d\n", num_partitions);
		std::cerr << "Error determining partition boundaries." << std::endl;
		std::exit(1);
	}

	// Reset file pointer to beginning
	fseek(f, header_bytes, SEEK_SET);
}

void _BM25::determine_partition_boundaries_json() {
	// Same as csv for now. Assuming newline delimited json.
	determine_partition_boundaries_csv();
}

void _BM25::proccess_csv_header() {
	// Iterate over first line to get column names.
	// If column name matches search_col, set search_column_index.

	FILE* f = reference_file_handles[0];
	char* line = NULL;
	size_t len = 0;
	// search_col_idx = -1;

	fseek(f, 0, SEEK_SET);

	// Get col names
	ssize_t read = getline(&line, &len, f);
	std::istringstream iss(line);
	std::string value;
	while (std::getline(iss, value, ',')) {
		if (value.find("\n") != std::string::npos) {
			value.erase(value.find("\n"));
		}
		columns.push_back(value);
		for (const auto& col : search_cols) {
			if (value == col) {
				search_col_idxs.push_back(columns.size() - 1);
			}
		}
	}

	if (search_col_idxs.empty()) {
		std::cerr << "Search column not found in header" << std::endl;
		std::cerr << "Cols found:  ";
		for (size_t i = 0; i < columns.size(); ++i) {
			std::cerr << columns[i] << ",";
		}
		std::cerr << std::endl;
		std::cout << std::flush;
		exit(1);
	}

	std::sort(search_col_idxs.begin(), search_col_idxs.end());

	header_bytes = read;
	free(line);
}

inline RLEElement_u8 init_rle_element_u8(uint8_t value) {
	RLEElement_u8 rle;
	rle.num_repeats = 1;
	rle.value = value;
	return rle;
}

inline uint64_t get_rle_element_u8_size(const RLEElement_u8& rle_element) {
	return (uint64_t)rle_element.num_repeats;
}

bool check_rle_u8_row_size(const std::vector<RLEElement_u8>& rle_row, uint64_t max_size) {
	uint64_t size = 0;
	for (const auto& rle_element : rle_row) {
		size += get_rle_element_u8_size(rle_element);
		if (size >= max_size) {
			return true;
		}
	}
	return false;
}

inline uint64_t get_rle_u8_row_size(const std::vector<RLEElement_u8>& rle_row) {
	uint64_t size = 0;
	for (const auto& rle_element : rle_row) {
		size += get_rle_element_u8_size(rle_element);
	}
	return size;
}

void add_rle_element_u8(std::vector<RLEElement_u8>& rle_row, uint8_t value) {
	if (rle_row.empty()) {
		rle_row.push_back(init_rle_element_u8(value));
	}
	else {
		if (rle_row.back().value == value) {
			++(rle_row.back().num_repeats);
		}
		else {
			RLEElement_u8 rle = init_rle_element_u8(value);
			rle_row.push_back(rle);
		}
	}
}

uint32_t _BM25::process_doc_partition(
		const char* doc,
		const char terminator,
		uint64_t doc_id,
		uint32_t& unique_terms_found,
		uint16_t partition_id,
		uint16_t col_idx
		) {
	BM25Partition& IP = index_partitions[partition_id];
	InvertedIndex& II = IP.II[col_idx];

	uint32_t char_idx = 0;

	std::string term = "";

	robin_hood::unordered_flat_map<uint64_t, uint8_t> terms_seen;

	// Split by commas not inside double quotes
	uint64_t doc_size = 0;
	while (true) {
		if (char_idx > 1048576) {
			std::cout << "Search field not found on line: " << doc_id << std::endl;
			std::cout << "Doc: " << doc << std::endl;
			std::cout << std::flush;
			std::exit(1);
		}
		if (doc[char_idx] == '\\') {
			++char_idx;
			term += toupper(doc[char_idx]);
			++char_idx;
			continue;
		}

		if (terminator == ',' && doc[char_idx] == ',') {
			++char_idx;
			break;
		}

		if (doc[char_idx] == '\n') {
			++char_idx;
			break;
		}

		if (terminator == '"' && doc[char_idx] == '"') {
			if (doc[++char_idx] == ',') {
				break;
			}

			if (doc[char_idx] == terminator) {
				term += terminator;
				++char_idx;
				continue;
			}
		}

		if (doc[char_idx] == ' ' && term == "") {
			++char_idx;
			continue;
		}

		if (doc[char_idx] == ' ') {
			if ((stop_words.find(term) != stop_words.end()) || !is_valid_token(term)) {
				term.clear();
				++char_idx;
				++doc_size;
				continue;
			}

			auto [it, add] = IP.unique_term_mapping[col_idx].try_emplace(
					term, 
					unique_terms_found
					);
			if (add) {
				// New term
				terms_seen.insert({it->second, 1});
				II.inverted_index_compressed.emplace_back();
				II.prev_doc_ids.push_back(0);
				II.doc_freqs.push_back(1);
				++unique_terms_found;
			}
			else {
				// Term already exists
				if (terms_seen.find(it->second) == terms_seen.end()) {
					terms_seen.insert({it->second, 1});
					++(II.doc_freqs[it->second]);
				}
				else {
					++(terms_seen[it->second]);
				}
			}

			++doc_size;
			term.clear();

			++char_idx;
			continue;
		}

		term += toupper(doc[char_idx]);
		++char_idx;
	}

	if (term != "") {
		if ((stop_words.find(term) == stop_words.end()) && is_valid_token(term)) {
			auto [it, add] = IP.unique_term_mapping[col_idx].try_emplace(
					term, 
					unique_terms_found
					);

			if (add) {
				// New term
				terms_seen.insert({it->second, 1});
				II.inverted_index_compressed.emplace_back();
				II.prev_doc_ids.push_back(0);
				II.doc_freqs.push_back(1);
				++unique_terms_found;
			}
			else {
				// Term already exists
				if (terms_seen.find(it->second) == terms_seen.end()) {
					terms_seen.insert({it->second, 1});
					++(II.doc_freqs[it->second]);
				}
				else {
					++(terms_seen[it->second]);
				}
			}
		}
		++doc_size;
	}

	if (doc_id == IP.doc_sizes.size() - 1) {
		IP.doc_sizes[doc_id] += (uint16_t)doc_size;
	}
	else {
		IP.doc_sizes.push_back((uint16_t)doc_size);
	}

	for (const auto& [term_idx, tf] : terms_seen) {
		compress_uint64_differential_single(
				II.inverted_index_compressed[term_idx].doc_ids,
				doc_id,
				II.prev_doc_ids[term_idx]
				);
		add_rle_element_u8(
				II.inverted_index_compressed[term_idx].term_freqs, 
				tf
				);
		II.prev_doc_ids[term_idx] = doc_id;
	}

	return char_idx;
}


void _BM25::update_progress(int line_num, int num_lines, uint16_t partition_id) {
    const int bar_width = 121;

    float percentage = static_cast<float>(line_num) / num_lines;
    int pos = bar_width * percentage;

    std::string bar;
    if (pos == bar_width) {
        bar = "[" + std::string(bar_width - 1, '=') + ">" + "]";
    } else {
        bar = "[" + std::string(pos, '=') + ">" + std::string(bar_width - pos - 1, ' ') + "]";
    }

    std::string info = std::to_string(static_cast<int>(percentage * 100)) + "% " +
                       std::to_string(line_num) + " / " + std::to_string(num_lines) + " docs read";
    std::string output = "Partition " + std::to_string(partition_id + 1) + ": " + bar + " " + info;

    {
        std::lock_guard<std::mutex> lock(progress_mutex);

        progress_bars.resize(std::max(progress_bars.size(), static_cast<size_t>(partition_id + 1)));
        progress_bars[partition_id] = output;

        std::cout << "\033[s";  // Save the cursor position

		// Move the cursor to the appropriate position for this partition
        std::cout << "\033[" << (partition_id + 1 + init_cursor_row) << ";1H";

        std::cout << output << std::endl;

        std::cout << "\033[u";  // Restore the cursor to the original position after updating
        std::cout << std::flush;
    }
}

void _BM25::finalize_progress_bar() {
    std::cout << "\033[" << (num_partitions + 1 + init_cursor_row) << ";1H";
	fflush(stdout);
}


IIRow get_II_row(
		InvertedIndex* II, 
		uint64_t term_idx
		) {
	IIRow row;

	row.df = get_rle_u8_row_size(II->inverted_index_compressed[term_idx].term_freqs);

	decompress_uint64(
			II->inverted_index_compressed[term_idx].doc_ids,
			row.doc_ids
			);

	// Convert doc_ids back to absolute values
	for (size_t i = 1; i < row.doc_ids.size(); ++i) {
		row.doc_ids[i] += row.doc_ids[i - 1];
	}

	// Get term frequencies
	for (size_t i = 0; i < II->inverted_index_compressed[term_idx].term_freqs.size(); ++i) {
		for (size_t j = 0; j < II->inverted_index_compressed[term_idx].term_freqs[i].num_repeats; ++j) {
			row.term_freqs.push_back(
					(float)II->inverted_index_compressed[term_idx].term_freqs[i].value
					);
		}
	}

	return row;
}

static inline void get_key(
		const char* line, 
		int& char_idx,
		std::string& key
		) {
	int start = char_idx;
	while (line[char_idx] != ':') {
		if (line[char_idx] == '\\') {
			char_idx += 2;
			continue;
		}
		++char_idx;
	}
	key = std::string(&line[start], char_idx - start - 1);
}

static inline void scan_to_next_key(
		const char* line, 
		int& char_idx
		) {
	while (line[char_idx] != ',') {
		if (line[char_idx] == '}') return;

		if (line[char_idx] == '\\') {
			char_idx += 2;
			continue;
		}

		if (line[char_idx] == '"') {
			++char_idx;

			// Scan to next unescaped quote
			while (line[char_idx] != '"') {
				if (line[char_idx] == '\\') {
					char_idx += 2;
					continue;
				}
				++char_idx;
			}
		}
		++char_idx;
	}
	++char_idx;
}

void _BM25::write_bloom_filters(uint16_t partition_id) {
	uint32_t min_df_bloom;
	if (bloom_df_threshold <= 1.0f) {
		min_df_bloom = (uint32_t)(bloom_df_threshold * index_partitions[partition_id].num_docs);
	} else {
		min_df_bloom = (uint32_t)bloom_df_threshold / (0.5f * num_partitions);
	}
	const uint16_t TOP_K = 100;

	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		BM25Partition& IP = index_partitions[partition_id];
		InvertedIndex& II = IP.II[col_idx];

		for (uint64_t idx = 0; idx < II.doc_freqs.size(); ++idx) {
			uint32_t df = II.doc_freqs[idx];
			if (df < min_df_bloom) continue;

			if (DEBUG) printf("Term: %s\n", IP.reverse_term_mapping[col_idx][idx].c_str());
			BloomEntry bloom_entry = init_bloom_entry(df, bloom_fpr);

			IIRow row = get_II_row(&II, idx);

			// partial sort TOP_K term_freqs descending. Get idxs
			std::vector<uint32_t> idxs(row.term_freqs.size());
			std::iota(idxs.begin(), idxs.end(), 0);
			std::partial_sort(
					idxs.begin(), 
					idxs.begin() + TOP_K, 
					idxs.end(), 
					[&row](uint32_t i1, uint32_t i2) {
						return row.term_freqs[i1] > row.term_freqs[i2];
					}
					);
			bloom_entry.topk_doc_ids.reserve(TOP_K);
			for (uint16_t i = 0; i < TOP_K; ++i) {
				bloom_entry.topk_doc_ids.push_back(row.doc_ids[idxs[i]]);
				bloom_entry.topk_term_freqs.push_back(row.doc_ids[idxs[i]]);
			}

			II.inverted_index_compressed[idx].doc_ids.clear();
			II.inverted_index_compressed[idx].term_freqs.clear();

			for (const uint64_t doc_id : row.doc_ids) {
				bloom_entry.bloom_filter.put(doc_id);
			}

			II.bloom_filters.insert({idx, bloom_entry});
		}
	}
}

void _BM25::read_json(uint64_t start_byte, uint64_t end_byte, uint16_t partition_id) {
	FILE* f = reference_file_handles[partition_id];
	BM25Partition& IP = index_partitions[partition_id];

	// Quickly count number of lines in file
	uint64_t num_lines = 0;
	uint64_t total_bytes_read = 0;
	char buf[1024 * 64];

	if (fseek(f, start_byte, SEEK_SET) != 0) {
		std::cerr << "Error seeking file." << std::endl;
		std::exit(1);
	}

	while (total_bytes_read < (end_byte - start_byte)) {
		size_t bytes_read = fread(buf, 1, sizeof(buf), f);
		if (bytes_read == 0) {
			break;
		}

		for (size_t i = 0; i < bytes_read; ++i) {
			if (buf[i] == '\n') {
				++num_lines;
			}
			if (++total_bytes_read >= (end_byte - start_byte)) {
				break;
			}
		}
	}

	IP.num_docs = num_lines;

	// Reset file pointer to beginning
	fseek(f, start_byte, SEEK_SET);

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;
	uint64_t byte_offset = start_byte;

	// uint32_t unique_terms_found = 0;
	std::vector<uint32_t> unique_terms_found(search_cols.size(), 0);

	const int UPDATE_INTERVAL = 10000;
	while ((read = getline(&line, &len, f)) != -1) {

		if (byte_offset >= end_byte) {
			break;
		}

		if (!DEBUG) {
			if (line_num % UPDATE_INTERVAL == 0) {
				update_progress(line_num, num_lines, partition_id);
			}
		}
		if (strlen(line) == 0) {
			std::cout << "Empty line found" << std::endl;
			std::exit(1);
		}

		IP.line_offsets.push_back(byte_offset);
		byte_offset += read;

		std::string key = "";
		bool found = false;

		// First char is always `{`
		int char_idx = 1;
		while (true) {
			start:
				while (line[char_idx] == ' ') ++char_idx;

				// Found key. Match against search_col.
				if (line[char_idx] == '"') {
					// Iter over quote.
					++char_idx;

					// Get key. char_idx will now be on a ':'.
					get_key(line, char_idx, key); ++char_idx;

					uint16_t search_col_idx = 0;
					for (const auto& search_col : search_cols) {
						if (key == search_col) {
							found = true;

							// Go to first char of value.
							while (line[char_idx] == ' ') ++char_idx;

							if (line[char_idx] != '"') {
								// Assume null. Must be string values.
								scan_to_next_key(line, char_idx);
								key.clear();
								++search_col_idx;
								goto start;
							}

							// Iter over quote.
							++char_idx;

							char_idx += process_doc_partition(
									&line[char_idx], 
									'"', 
									line_num, 
									unique_terms_found[search_col_idx], 
									partition_id,
									search_col_idx++
									); ++char_idx;
							scan_to_next_key(line, char_idx);
							key.clear();
							goto start;
						}
					}

					key.clear();
					scan_to_next_key(line, char_idx);
					++search_col_idx;
				}
				else if (line[char_idx] == '}') {
					if (!found) {
						std::cout << "Search field not found on line: " << line_num << std::endl;
						std::cout << std::flush;
						std::exit(1);
					}
					// Success. Break.
					break;
				}
				else if (char_idx > 1048576) {
					std::cout << "Search field not found on line: " << line_num << std::endl;
					std::cout << std::flush;
					std::exit(1);
				}
				else {
					std::cerr << "Invalid json." << std::endl;
					std::cout << "Line: " << line << std::endl;
					std::cout << &line[char_idx] << std::endl;
					std::cout << char_idx << std::endl;
					std::cout << std::flush;
					std::exit(1);
				}
		}

		if (!DEBUG) {
			if (line_num % UPDATE_INTERVAL == 0) update_progress(line_num, num_lines, partition_id);
		}

		++line_num;
	}

	if (!DEBUG) update_progress(line_num, num_lines, partition_id);

	free(line);

	if (DEBUG) {
		for (uint16_t col = 0; col < search_cols.size(); ++col) {
			std::cout << "Vocab size: " << unique_terms_found[col] << std::endl;
		}
	}

	IP.num_docs = num_lines;

	// Calc avg_doc_size
	double avg_doc_size = 0;
	for (const auto& size : IP.doc_sizes) {
		avg_doc_size += (double)size;
	}
	IP.avg_doc_size = (float)(avg_doc_size / num_lines);
}

void _BM25::read_csv(uint64_t start_byte, uint64_t end_byte, uint16_t partition_id) {
	FILE* f = reference_file_handles[partition_id];
	BM25Partition& IP = index_partitions[partition_id];

	// Quickly count number of lines in file
	uint64_t num_lines = 0;
	uint64_t total_bytes_read = 0;
	char buf[1024 * 64];

	if (fseek(f, start_byte, SEEK_SET) != 0) {
		std::cerr << "Error seeking file." << std::endl;
		std::exit(1);
	}

	while (total_bytes_read < (end_byte - start_byte)) {
		size_t bytes_read = fread(buf, 1, sizeof(buf), f);
		if (bytes_read == 0) {
			break;
		}

		for (size_t i = 0; i < bytes_read; ++i) {
			if (buf[i] == '\n') {
				++num_lines;
			}
			if (++total_bytes_read >= (end_byte - start_byte)) {
				break;
			}
		}
	}

	IP.num_docs = num_lines;

	// Reset file pointer to beginning
	if (fseek(f, start_byte, SEEK_SET) != 0) {
		std::cerr << "Error seeking file." << std::endl;
		std::exit(1);
	}

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;
	uint64_t byte_offset = start_byte;

	// uint32_t unique_terms_found = 0;
	std::vector<uint32_t> unique_terms_found(search_cols.size());

	// Small string optimization limit on most platforms
	std::string doc = "";
	doc.reserve(22);

	char end_delim = ',';

	const int UPDATE_INTERVAL = 10000;
	while ((read = getline(&line, &len, f)) != -1) {

		if (byte_offset >= end_byte) {
			break;
		}

		if (!DEBUG) {
			if (line_num % UPDATE_INTERVAL == 0) update_progress(line_num, num_lines, partition_id);
		}

		IP.line_offsets.push_back(byte_offset);
		byte_offset += read;

		int char_idx = 0;
		int col_idx  = 0;
		uint16_t _search_col_idx = 0;
		for (const auto& search_col_idx : search_col_idxs) {

			if (search_col_idx == (int)columns.size() - 1) {
				end_delim = '\n';
			}
			else {
				end_delim = ',';
			}

			// Iterate of line chars until we get to relevant column.
			while (col_idx != search_col_idx) {
				if (line[char_idx] == '\\') {
					char_idx += 2;
					continue;
				}

				if (line[char_idx] == '\n') {
					printf("Newline found before end.\n");
					printf("Col idx: %d\n", col_idx);
					printf("Line: %s", line);
					exit(1);
				}

				if (line[char_idx] == '"') {
					// Skip to next unescaped quote
					++char_idx;

					while (1) {
						if (line[char_idx] == '"') {
							if (line[char_idx + 1] == '"') {
								char_idx += 2;
								continue;
							} 
							else {
								++char_idx;
								break;
							}
						}
						++char_idx;
					}
				}

				if (line[char_idx] == ',') ++col_idx;
				++char_idx;
			}
			++col_idx;

			// Split by commas not inside double quotes
			if (line[char_idx] == '"') {
				++char_idx;
				char_idx += process_doc_partition(
					&line[char_idx],
					'"',
					line_num,
					unique_terms_found[_search_col_idx],
					partition_id,
					_search_col_idx
					); ++char_idx;
				++_search_col_idx;
				continue;
			}

			char_idx += process_doc_partition(
				&line[char_idx], 
				end_delim,
				line_num, 
				unique_terms_found[_search_col_idx], 
				partition_id,
				_search_col_idx
				); ++char_idx;
			++_search_col_idx;
		}
		++line_num;
	}
	if (!DEBUG) update_progress(line_num + 1, num_lines, partition_id);

	if (DEBUG) {
		for (uint32_t col = 0; col < search_col_idxs.size(); ++col) {
			std::cout << "Vocab size " << col << ": " << unique_terms_found[col] << std::endl;
		}
	}


	IP.num_docs = IP.doc_sizes.size();

	// Calc avg_doc_size
	double avg_doc_size = 0;
	for (const auto& size : IP.doc_sizes) {
		avg_doc_size += (double)size;
	}
	IP.avg_doc_size = (float)(avg_doc_size / num_lines);
}

void _BM25::read_in_memory(
		std::vector<std::vector<std::string>>& documents,
		uint64_t start_idx, 
		uint64_t end_idx, 
		uint16_t partition_id
		) {
	BM25Partition& IP = index_partitions[partition_id];

	IP.num_docs = end_idx - start_idx;

	std::vector<uint32_t> unique_terms_found(search_cols.size(), 0);

	uint32_t cntr = 0;
	const int UPDATE_INTERVAL = 10000;

	for (uint64_t line_num = start_idx; line_num < end_idx; ++line_num) {
		if (!DEBUG) {
			if (cntr % UPDATE_INTERVAL == 0) {
				update_progress(cntr, IP.num_docs, partition_id);
			}
		}

		for (uint16_t col = 0; col < search_cols.size(); ++col) {
			std::string& doc = documents[line_num][col];
			process_doc_partition(
				(doc + "\n").c_str(),
				'\n',
				cntr, 
				unique_terms_found[col],
				partition_id,
				col
				);
		}
		++cntr;
	}
	if (!DEBUG) update_progress(cntr + 1, IP.num_docs, partition_id);

	// Calc avg_doc_size
	double avg_doc_size = 0;
	for (const auto& size : IP.doc_sizes) {
		avg_doc_size += (double)size;
	}
	IP.avg_doc_size = (float)(avg_doc_size / IP.num_docs);
}


std::vector<std::pair<std::string, std::string>> _BM25::get_csv_line(int line_num, uint16_t partition_id) {
	FILE* f = reference_file_handles[partition_id];
	BM25Partition& IP = index_partitions[partition_id];

	// seek from FILE* reference_file which is already open
	fseek(f, IP.line_offsets[line_num], SEEK_SET);
	char* line = NULL;
	size_t len = 0;
	ssize_t read = getline(&line, &len, f);

	// Create effective json by combining column names with values split by commas
	std::vector<std::pair<std::string, std::string>> row;
	std::string cell;
	bool in_quotes = false;
	size_t col_idx = 0;

	for (size_t i = 0; i < (size_t)read - 1; ++i) {
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


std::vector<std::pair<std::string, std::string>> _BM25::get_json_line(int line_num, uint16_t partition_id) {
	FILE* f = reference_file_handles[partition_id];
	BM25Partition& IP = index_partitions[partition_id];

	// seek from FILE* reference_file which is already open
	fseek(f, IP.line_offsets[line_num], SEEK_SET);
	char* line = NULL;
	size_t len = 0;
	getline(&line, &len, f);

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

		// Go to first char of value.
		while (line[char_idx] == '"' || line[char_idx] == ' ') {
			++char_idx;
		}

		while (line[char_idx] != '}' || line[char_idx] != '"' || line[char_idx] != ',') {
			if (line[char_idx] == '\\') {
				++char_idx;
				second += line[char_idx];
				++char_idx;
				continue;
			}
			else if (line[char_idx] == '}') {
				second += line[char_idx];
				row.emplace_back(first, second);
				return row;
			}
			second += line[char_idx];
			++char_idx;
		}
		++char_idx;
		if (line[char_idx] == '}') {
			return row;
		}
	}
	return row;
}

void _BM25::save_index_partition(
		std::string db_dir,
		uint16_t partition_id
		) {
	std::string UNIQUE_TERM_MAPPING_PATH = db_dir + "/unique_term_mapping.bin";
	std::string INVERTED_INDEX_PATH 	 = db_dir + "/inverted_index.bin";
	std::string DOC_SIZES_PATH 		     = db_dir + "/doc_sizes.bin";
	std::string LINE_OFFSETS_PATH 		 = db_dir + "/line_offsets.bin";

	BM25Partition& IP = index_partitions[partition_id];


	for (uint16_t col_idx = 0; col_idx < search_col_idxs.size(); ++col_idx) {
		serialize_robin_hood_flat_map_string_u32(
				IP.unique_term_mapping[col_idx],
				UNIQUE_TERM_MAPPING_PATH + "_" + std::to_string(partition_id) + "_" + std::to_string(col_idx)
				);
		serialize_inverted_index(
				IP.II[col_idx], 
				INVERTED_INDEX_PATH + "_" + std::to_string(partition_id) + "_" + std::to_string(col_idx)
				);
	}
	serialize_vector_u16(IP.doc_sizes, DOC_SIZES_PATH + "_" + std::to_string(partition_id));

	std::vector<uint8_t> compressed_line_offsets;
	compressed_line_offsets.reserve(IP.line_offsets.size() * 2);
	compress_uint64(IP.line_offsets, compressed_line_offsets);

	serialize_vector_u8(
			compressed_line_offsets, LINE_OFFSETS_PATH + "_" + std::to_string(partition_id)
			);
}

void _BM25::load_index_partition(
		std::string db_dir,
		uint16_t partition_id
		) {
	std::string UNIQUE_TERM_MAPPING_PATH = db_dir + "/unique_term_mapping.bin";
	std::string INVERTED_INDEX_PATH 	 = db_dir + "/inverted_index.bin";
	std::string DOC_SIZES_PATH 		     = db_dir + "/doc_sizes.bin";
	std::string LINE_OFFSETS_PATH 		 = db_dir + "/line_offsets.bin";

	BM25Partition& IP = index_partitions[partition_id];
	IP.unique_term_mapping.resize(search_col_idxs.size());
	IP.II.resize(search_col_idxs.size());

	for (uint16_t col_idx = 0; col_idx < search_col_idxs.size(); ++col_idx) {
		deserialize_robin_hood_flat_map_string_u32(
				IP.unique_term_mapping[col_idx],
				UNIQUE_TERM_MAPPING_PATH + "_" + std::to_string(partition_id) + "_" + std::to_string(col_idx)
				);
		deserialize_inverted_index(
				IP.II[col_idx], 
				INVERTED_INDEX_PATH + "_" + std::to_string(partition_id) + "_" + std::to_string(col_idx)
				);
	}
	deserialize_vector_u16(IP.doc_sizes, DOC_SIZES_PATH + "_" + std::to_string(partition_id));

	std::vector<uint8_t> compressed_line_offsets;
	deserialize_vector_u8(
			compressed_line_offsets, LINE_OFFSETS_PATH + "_" + std::to_string(partition_id)
			);

	decompress_uint64(compressed_line_offsets, IP.line_offsets);
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
	std::string METADATA_PATH = db_dir + "/metadata.bin";

	std::vector<std::thread> threads;
	for (uint16_t partition_id = 0; partition_id < num_partitions; ++partition_id) {
		threads.push_back(std::thread(&_BM25::save_index_partition, this, db_dir, partition_id));
	}

	for (auto& thread : threads) {
		thread.join();
	}

	// Save partition boundaries
	std::string PARTITION_BOUNDARY_PATH = db_dir + "/partition_boundaries.bin";
	serialize_vector_u64(partition_boundaries, PARTITION_BOUNDARY_PATH);

	// Serialize smaller members.
	std::ofstream out_file(METADATA_PATH, std::ios::binary);
	if (!out_file) {
		std::cerr << "Error opening file for writing.\n";
		return;
	}

	// Write basic types directly
	out_file.write(reinterpret_cast<const char*>(&num_docs), sizeof(num_docs));
	out_file.write(reinterpret_cast<const char*>(&bloom_df_threshold), sizeof(bloom_df_threshold));
	out_file.write(reinterpret_cast<const char*>(&bloom_fpr), sizeof(bloom_fpr));
	out_file.write(reinterpret_cast<const char*>(&k1), sizeof(k1));
	out_file.write(reinterpret_cast<const char*>(&b), sizeof(b));
	out_file.write(reinterpret_cast<const char*>(&num_partitions), sizeof(num_partitions));

	for (uint16_t partition_id = 0; partition_id < num_partitions; ++partition_id) {
		BM25Partition& IP = index_partitions[partition_id];

		out_file.write(reinterpret_cast<const char*>(&IP.avg_doc_size), sizeof(IP.avg_doc_size));
	}

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

	// Write search_cols std::vector<std::string>
	columns_size = search_cols.size();
	out_file.write(reinterpret_cast<const char*>(&columns_size), sizeof(columns_size));
	for (const auto& search_col : search_cols) {
		size_t search_col_length = search_col.size();
		out_file.write(reinterpret_cast<const char*>(&search_col_length), sizeof(search_col_length));
		out_file.write(search_col.data(), search_col_length);
	}

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
	std::string PARTITION_BOUNDARY_PATH  = db_dir + "/partition_boundaries.bin";

	// Load smaller members.
	std::ifstream in_file(METADATA_PATH, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read basic types directly
    in_file.read(reinterpret_cast<char*>(&num_docs), sizeof(num_docs));
    in_file.read(reinterpret_cast<char*>(&bloom_df_threshold), sizeof(bloom_df_threshold));
    in_file.read(reinterpret_cast<char*>(&bloom_fpr), sizeof(bloom_fpr));
    in_file.read(reinterpret_cast<char*>(&k1), sizeof(k1));
    in_file.read(reinterpret_cast<char*>(&b), sizeof(b));
	in_file.read(reinterpret_cast<char*>(&num_partitions), sizeof(num_partitions));

	// Load partition boundaries
	deserialize_vector_u64(partition_boundaries, PARTITION_BOUNDARY_PATH);

	index_partitions.clear();
	index_partitions.resize(num_partitions);

	// Load rest of metadata.
	for (uint16_t partition_id = 0; partition_id < num_partitions; ++partition_id) {
		BM25Partition& IP = index_partitions[partition_id];

		in_file.read(reinterpret_cast<char*>(&IP.avg_doc_size), sizeof(IP.avg_doc_size));
	}

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

    // Read search_cols std::vector<std::string>
	in_file.read(reinterpret_cast<char*>(&columns_size), sizeof(columns_size));
	search_cols.resize(columns_size);
	for (auto& search_col : search_cols) {
		size_t search_col_length;
		in_file.read(reinterpret_cast<char*>(&search_col_length), sizeof(search_col_length));
		search_col.resize(search_col_length);
		in_file.read(&search_col[0], search_col_length);
	}

	search_col_idxs.resize(search_cols.size());

    in_file.close();

	std::vector<std::thread> threads;

	for (uint16_t partition_id = 0; partition_id < num_partitions; ++partition_id) {
		threads.push_back(std::thread(&_BM25::load_index_partition, this, db_dir, partition_id));
	}

	for (auto& thread : threads) {
		thread.join();
	}

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Loaded in " << elapsed_seconds.count() << "s" << std::endl;
	}
}


_BM25::_BM25(
		std::string filename,
		std::vector<std::string> search_cols,
		float  bloom_df_threshold,
		double bloom_fpr,
		float  k1,
		float  b,
		uint16_t num_partitions,
		const std::vector<std::string>& _stop_words
		) : bloom_df_threshold(bloom_df_threshold),
			bloom_fpr(bloom_fpr),
			k1(k1), 
			b(b),
			num_partitions(num_partitions),
			search_cols(search_cols), 
			filename(filename) {


	for (const std::string& stop_word : _stop_words) {
		stop_words.insert(stop_word);
	}

	// Open file handles
	for (uint16_t i = 0; i < num_partitions; ++i) {
		FILE* f = fopen(filename.c_str(), "r");
		if (f == NULL) {
			std::cerr << "Unable to open file: " << filename << std::endl;
			exit(1);
		}
		reference_file_handles.push_back(f);
	}

	auto overall_start = std::chrono::high_resolution_clock::now();

	std::vector<std::thread> threads;

	index_partitions.resize(num_partitions);
	for (uint16_t i = 0; i < num_partitions; ++i) {
		index_partitions[i].II.resize(search_cols.size());
		index_partitions[i].unique_term_mapping.resize(search_cols.size());
	}

	num_docs = 0;

	progress_bars.resize(num_partitions);
	bool is_terminal = isatty(fileno(stdout));

	int col;
	if (is_terminal) {
		get_cursor_position(init_cursor_row, col);
		get_terminal_size(terminal_height, col);

		if (terminal_height - init_cursor_row < num_partitions + 1) {
			// Scroll and reposition cursor
			std::cout << "\x1b[" << num_partitions + 1 << "S";
			init_cursor_row -= num_partitions + 1;
		}
	}

	// Read file to get documents, line offsets, and columns
	if (filename.substr(filename.size() - 3, 3) == "csv") {

		proccess_csv_header();
		determine_partition_boundaries_csv();

		// Launch num_partitions threads to read csv file
		for (uint16_t i = 0; i < num_partitions; ++i) {
			threads.push_back(std::thread(
				[this, i] {
					read_csv(partition_boundaries[i], partition_boundaries[i + 1], i);
					if (DEBUG) {
						BM25Partition& IP = index_partitions[i];
						IP.reverse_term_mapping.resize(this->search_cols.size());
						for (uint16_t col = 0; col < this->search_cols.size(); ++col) {
							for (const auto& term : IP.unique_term_mapping[col]) {
								IP.reverse_term_mapping[col].insert({term.second, term.first});
							}
						}
					}
					write_bloom_filters(i);
				}
			));
		}

		file_type = CSV;
	}
	else if (filename.substr(filename.size() - 4, 4) == "json") {
		header_bytes = 0;
		determine_partition_boundaries_json();

		// Launch num_partitions threads to read json file
		for (uint16_t i = 0; i < num_partitions; ++i) {
			threads.push_back(std::thread(
				[this, i] {
					read_json(partition_boundaries[i], partition_boundaries[i + 1], i);
					if (DEBUG) {
						BM25Partition& IP = index_partitions[i];
						IP.reverse_term_mapping.resize(this->search_cols.size());
						for (uint16_t col = 0; col < this->search_cols.size(); ++col) {
							for (const auto& term : IP.unique_term_mapping[col]) {
								IP.reverse_term_mapping[col].insert({term.second, term.first});
							}
						}
					}
					write_bloom_filters(i);
				}
			));
		}
		file_type = JSON;
	}
	else {
		std::cout << "Only csv and json files are supported." << std::endl;
		std::exit(1);
	}


	for (auto& thread : threads) {
		thread.join();
	}

	num_docs = 0;
	for (uint16_t i = 0; i < num_partitions; ++i) {
		num_docs += index_partitions[i].num_docs;
	}

	if (!DEBUG) finalize_progress_bar();

	uint64_t total_size = 0;
	uint32_t unique_terms_found = 0;
	uint64_t bloom_filters_size = 0;
	uint64_t total_bloom_filters = 0;
	for (uint16_t i = 0; i < num_partitions; ++i) {
		BM25Partition& IP = index_partitions[i];

		uint64_t part_size = 0;
		for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
			for (const auto& row : IP.II[col_idx].inverted_index_compressed) {
				part_size += sizeof(uint8_t) * row.doc_ids.size();
				part_size += sizeof(RLEElement_u8) * row.term_freqs.size();
			}
			unique_terms_found += IP.unique_term_mapping[col_idx].size();
			total_bloom_filters += IP.II[col_idx].bloom_filters.size();
			for (const auto& bf : index_partitions[i].II[col_idx].bloom_filters) {
				bloom_filters_size += bf.second.bloom_filter.size() / 8;
			}
		}
		total_size += part_size;

	}
	total_size /= 1024 * 1024;
	uint64_t vocab_size = unique_terms_found * (4 + 5 + 1) / 1048576;
	uint64_t line_offsets_size = num_docs * 8 / 1048576;
	uint64_t doc_sizes_size = num_docs * 2 / 1048576;
	uint64_t inverted_index_size = total_size;
	bloom_filters_size /= 1048576;
	total_size = vocab_size + line_offsets_size + doc_sizes_size + inverted_index_size + bloom_filters_size;

	std::cout << "Total size of vocab mappings:  ~" << vocab_size << "MB" << std::endl;
	std::cout << "Total size of line offsets:     " << line_offsets_size << "MB" << std::endl;
	std::cout << "Total size of doc sizes:        " << doc_sizes_size << "MB" << std::endl;
	std::cout << "Total size of inverted indexes: " << inverted_index_size << "MB" << std::endl;
	std::cout << "Total size of bloom filters:    " << bloom_filters_size << "MB" << std::endl;
	std::cout << "--------------------------------------" << std::endl;
	std::cout << "Approx total in-memory size:    " << total_size << "MB" << std::endl << std::endl;

	std::cout << "Total number of documents:      " << num_docs << std::endl;
	std::cout << "Total number of unique terms:   " << unique_terms_found << std::endl;
	std::cout << "Total number of bloom filters:  " << total_bloom_filters << std::endl;

	if (DEBUG) {
		auto read_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> read_elapsed_seconds = read_end - overall_start;
		std::cout << "Read file in " << read_elapsed_seconds.count() << " seconds" << std::endl;
	}
}


_BM25::_BM25(
		std::vector<std::vector<std::string>>& documents,
		float  bloom_df_threshold,
		double bloom_fpr,
		float  k1,
		float  b,
		uint16_t num_partitions,
		const std::vector<std::string>& _stop_words
		) : bloom_df_threshold(bloom_df_threshold),
			bloom_fpr(bloom_fpr),
			k1(k1), 
			b(b),
			num_partitions(num_partitions) {
	
	for (const std::string& stop_word : _stop_words) {
		stop_words.insert(stop_word);
	}

	filename = "in_memory";
	file_type = IN_MEMORY;

	num_docs = documents.size();

	search_cols.resize(documents[0].size());
	search_col_idxs.resize(documents[0].size());

	index_partitions.resize(num_partitions);
	for (uint16_t i = 0; i < num_partitions; ++i) {
		index_partitions[i].II.resize(search_cols.size());
		index_partitions[i].unique_term_mapping.resize(search_cols.size());
	}

	index_partitions.resize(num_partitions);
	partition_boundaries.resize(num_partitions + 1);

	for (uint16_t i = 0; i < num_partitions; ++i) {
		partition_boundaries[i] = (uint64_t)i * (num_docs / num_partitions);
	}
	partition_boundaries[num_partitions] = num_docs;

	progress_bars.resize(num_partitions);
	bool is_terminal = isatty(fileno(stdout));

	int col;
	if (is_terminal) {
		get_cursor_position(init_cursor_row, col);
		get_terminal_size(terminal_height, col);

		if (terminal_height - init_cursor_row < num_partitions + 1) {
			// Scroll and reposition cursor
			std::cout << "\x1b[" << num_partitions + 1 << "S";
			init_cursor_row -= num_partitions + 1;
		}
	}

	std::vector<std::thread> threads;
	for (uint16_t i = 0; i < num_partitions; ++i) {
		threads.push_back(std::thread(
			[this, &documents, i] {
				read_in_memory(
						documents, 
						partition_boundaries[i], 
						partition_boundaries[i + 1], 
						i
						);
				write_bloom_filters(i);
			}
		));
	}

	for (auto& thread : threads) {
		thread.join();
	}

	if (!DEBUG) finalize_progress_bar();

	uint64_t total_size = 0;
	uint32_t unique_terms_found = 0;
	for (uint16_t i = 0; i < num_partitions; ++i) {
		BM25Partition& IP = index_partitions[i];

		uint64_t part_size = 0;
		for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
			for (const auto& row : IP.II[col_idx].inverted_index_compressed) {
				part_size += sizeof(uint8_t) * row.doc_ids.size();
				part_size += sizeof(RLEElement_u8) * row.term_freqs.size();
			}
			unique_terms_found += IP.unique_term_mapping[col_idx].size();
		}
		total_size += part_size;

	}
	total_size /= 1024 * 1024;
	uint64_t vocab_size = unique_terms_found * (4 + 5 + 1) / 1048576;
	uint64_t line_offsets_size = num_docs * 8 / 1048576;
	uint64_t doc_sizes_size = num_docs * 2 / 1048576;
	uint64_t inverted_index_size = total_size;
	total_size = vocab_size + line_offsets_size + doc_sizes_size + inverted_index_size;

	std::cout << "Total size of vocab mappings:  ~" << vocab_size << "MB" << std::endl;
	std::cout << "Total size of line offsets:     " << line_offsets_size << "MB" << std::endl;
	std::cout << "Total size of doc sizes:        " << doc_sizes_size << "MB" << std::endl;
	std::cout << "Total size of inverted indexes: " << inverted_index_size << "MB" << std::endl;
	std::cout << "--------------------------------------" << std::endl;
	std::cout << "Approx total in-memory size:    " << total_size << "MB" << std::endl << std::endl;

	std::cout << "Total number of documents:      " << num_docs << std::endl;
	std::cout << "Total number of unique terms:   " << unique_terms_found << std::endl;
}

inline float _BM25::_compute_bm25(
		uint64_t doc_id,
		float tf,
		float idf,
		uint16_t partition_id
		) {
	BM25Partition& IP = index_partitions[partition_id];

	float doc_size = IP.doc_sizes[doc_id];
	return idf * tf / (tf + k1 * (1 - b + b * doc_size / IP.avg_doc_size));
}

void _BM25::add_query_term(
		std::string& substr,
		std::vector<std::vector<uint64_t>>& term_idxs,
		uint16_t partition_id
		) {
	BM25Partition& IP = index_partitions[partition_id];

	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		robin_hood::unordered_map<std::string, uint32_t>& vocab = IP.unique_term_mapping[col_idx];

		if (vocab.find(substr) == vocab.end()) {
			continue;
		}

		if (stop_words.find(substr) != stop_words.end()) {
			continue;
		}

		term_idxs[col_idx].push_back(vocab[substr]);
	}
	substr.clear();
}


void _BM25::add_query_term_bloom(
		std::string& substr,
		std::vector<std::vector<uint64_t>>& low_df_term_idxs,
		std::vector<std::vector<uint64_t>>& high_df_term_idxs,
		std::vector<std::vector<BloomEntry>>& bloom_entries,
		uint16_t partition_id
		) {
	BM25Partition& IP = index_partitions[partition_id];

	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		robin_hood::unordered_map<std::string, uint32_t>& vocab = IP.unique_term_mapping[col_idx];

		auto it = vocab.find(substr);
		if (it == vocab.end()) {
			continue;
		}

		if (stop_words.find(substr) != stop_words.end()) {
			continue;
		}

		auto it2 = IP.II[col_idx].bloom_filters.find(it->second);
		if (it2 == IP.II[col_idx].bloom_filters.end()) {
			low_df_term_idxs[col_idx].push_back(it->second);
		}
		else {
			high_df_term_idxs[col_idx].push_back(it->second);
			bloom_entries[col_idx].push_back(it2->second);
		}
	}
	substr.clear();
}


std::vector<BM25Result> _BM25::_query_partition(
		std::string& query, 
		uint32_t k,
		uint32_t query_max_df,
		uint16_t partition_id,
		std::vector<float> boost_factors
		) {
	auto start = std::chrono::high_resolution_clock::now();
	std::vector<std::vector<uint64_t>> term_idxs(search_cols.size());
	BM25Partition& IP = index_partitions[partition_id];

	uint64_t doc_offset = (file_type == IN_MEMORY) ? partition_boundaries[partition_id] : 0;

	std::string substr = "";
	for (const char& c : query) {
		if (c != ' ') {
			substr += toupper(c); 
			continue;
		}

		add_query_term(substr, term_idxs, partition_id);	
	}
	if (!substr.empty()) {
		add_query_term(substr, term_idxs, partition_id);
	}

	if (term_idxs.size() == 0) return std::vector<BM25Result>();

	// Gather docs that contain at least one term from the query
	// Uses dynamic max_df for performance
	robin_hood::unordered_map<uint64_t, float> doc_scores;

	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		for (const uint64_t& term_idx : term_idxs[col_idx]) {
			float boost_factor = boost_factors[col_idx];

			uint64_t df = get_rle_u8_row_size(
					IP.II[col_idx].inverted_index_compressed[term_idx].term_freqs
					);

			if (df == 0 || df > query_max_df) {
				continue;
			}

			float idf = log((IP.num_docs - df + 0.5) / (df + 0.5));

			IIRow row = get_II_row(&IP.II[col_idx], term_idx);

			// Partial sort row.doc_ids by row.term_freqs to get top k
			for (uint64_t i = 0; i < df; ++i) {

				uint64_t doc_id  = row.doc_ids[i];
				float tf 		 = row.term_freqs[i];
				float bm25_score = _compute_bm25(doc_id, tf, idf, partition_id) * boost_factor;

				doc_id += doc_offset;
				if (doc_scores.find(doc_id) == doc_scores.end()) {
					doc_scores[doc_id] = bm25_score;
				}
				else {
					doc_scores[doc_id] += bm25_score;
				}
			}
		}
	}

	if (DEBUG) {
		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double, std::milli> elapsed_ms = end - start;
		std::cout << "Number of docs: " << doc_scores.size() << "   GATHER TIME: ";
		std::cout << elapsed_ms.count() << "ms" << std::endl;
		fflush(stdout);
	}
	
	if (doc_scores.size() == 0) {
		return std::vector<BM25Result>();
	}

	start = std::chrono::high_resolution_clock::now();
	std::priority_queue<
		BM25Result,
		std::vector<BM25Result>,
		_compare_bm25_result> top_k_docs;

	for (const auto& pair : doc_scores) {
		BM25Result result {
			.doc_id = pair.first,
			.score  = pair.second,
			.partition_id = partition_id
		};
		top_k_docs.push(result);
		if (top_k_docs.size() > k) {
			top_k_docs.pop();
		}
	}

	std::vector<BM25Result> result(top_k_docs.size());
	int idx = top_k_docs.size() - 1;
	while (!top_k_docs.empty()) {
		result[idx] = top_k_docs.top();
		top_k_docs.pop();
		--idx;
	}

	return result;
}

std::vector<BM25Result> _BM25::_query_partition_bloom(
		std::string& query, 
		uint32_t k,
		uint32_t query_max_df,
		uint16_t partition_id,
		std::vector<float> boost_factors
		) {
	auto start = std::chrono::high_resolution_clock::now();
	std::vector<std::vector<uint64_t>> low_df_term_idxs(search_cols.size());
	std::vector<std::vector<uint64_t>> high_df_term_idxs(search_cols.size());
	std::vector<std::vector<BloomEntry>> bloom_entries(search_cols.size());
	BM25Partition& IP = index_partitions[partition_id];

	uint64_t doc_offset = (file_type == IN_MEMORY) ? partition_boundaries[partition_id] : 0;

	std::string substr = "";
	for (const char& c : query) {
		if (c != ' ') {
			substr += toupper(c); 
			continue;
		}

		add_query_term_bloom(
				substr, 
				low_df_term_idxs, 
				high_df_term_idxs, 
				bloom_entries,
				partition_id
				);	
	}
	if (!substr.empty()) {
		add_query_term_bloom(
				substr, 
				low_df_term_idxs, 
				high_df_term_idxs, 
				bloom_entries,
				partition_id
				);	
	}

	uint16_t num_low_df_terms = 0;
	uint16_t num_high_df_terms = 0;
	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		num_low_df_terms  += low_df_term_idxs[col_idx].size();
		num_high_df_terms += high_df_term_idxs[col_idx].size();
	}
	if (num_low_df_terms + num_high_df_terms == 0) return std::vector<BM25Result>();

	// Score low_df terms first.
	robin_hood::unordered_map<uint64_t, float> doc_scores;

	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		for (const uint64_t& term_idx : low_df_term_idxs[col_idx]) {
			float boost_factor = boost_factors[col_idx];

			uint64_t df = IP.II[col_idx].doc_freqs[term_idx];

			if (df == 0 || df > query_max_df) {
				continue;
			}

			float idf = log((IP.num_docs - df + 0.5) / (df + 0.5));

			IIRow row = get_II_row(&IP.II[col_idx], term_idx);
			for (uint64_t i = 0; i < df; ++i) {

				uint64_t doc_id  = row.doc_ids[i];
				float tf 		 = row.term_freqs[i];
				float bm25_score = _compute_bm25(doc_id, tf, idf, partition_id) * boost_factor;

				doc_id += doc_offset;
				if (doc_scores.find(doc_id) == doc_scores.end()) {
					doc_scores[doc_id] = bm25_score;
				}
				else {
					doc_scores[doc_id] += bm25_score;
				}
			}
		}
	}

	// Now score high_df terms
	if (num_high_df_terms > 0) {
		if (doc_scores.size() == 0) {
			// Sort high_df_term_idxs by df.
			// Get top-k docs for lowest df term. Then use bloom scoring for the rest.
			std::vector<uint32_t> df_values;
			uint32_t min_df = UINT32_MAX;
			uint16_t min_df_col_idx;
			uint16_t min_df_term_idx;

			for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
				for (const uint64_t& term_idx : high_df_term_idxs[col_idx]) {
					uint32_t df = IP.II[col_idx].doc_freqs[term_idx];
					if (df < min_df) {
						min_df_col_idx = col_idx;
						min_df_term_idx = term_idx;
						min_df = df;
					}
					df_values.push_back(
								IP.II[col_idx].doc_freqs[term_idx]
								);
				}
			}

			// First score the term with the lowest df.
			float idf = log((IP.num_docs - min_df + 0.5) / (min_df + 0.5));
			BloomEntry& bloom_entry = bloom_entries[min_df_col_idx][min_df_term_idx];
			for (uint64_t i = 0; i < min_df; ++i) {

				uint64_t doc_id  = bloom_entry.topk_doc_ids[i];
				float tf 		 = bloom_entry.topk_term_freqs[i];
				float bm25_score = _compute_bm25(doc_id, tf, idf, partition_id) * boost_factors[min_df_col_idx];

				doc_id += doc_offset;
				if (doc_scores.find(doc_id) == doc_scores.end()) {
					doc_scores[doc_id] = bm25_score;
				}
				else {
					doc_scores[doc_id] += bm25_score;
				}
			}

			// Now score the rest using bloom filters.
			for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
				for (uint16_t idx = 0; idx < high_df_term_idxs[col_idx].size(); ++idx) {
					const uint64_t& term_idx = high_df_term_idxs[col_idx][idx];

					if (col_idx == min_df_col_idx && term_idx == min_df_term_idx) {
						continue;
					}

					BloomEntry& bloom_entry = bloom_entries[col_idx][idx];
					float df = IP.II[col_idx].doc_freqs[term_idx];
					float idf = log((IP.num_docs - df + 0.5) / (df + 0.5));

					for (auto& [doc_id, score] : doc_scores) {

						if (bloom_entry.bloom_filter.query(doc_id)) {
							score += _compute_bm25(
									doc_id, 
									1.0f,
									idf, 
									partition_id
									) * boost_factors[col_idx];
						}
					}
				}
			}
		} else {

			for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
				for (uint16_t idx = 0; idx < high_df_term_idxs[col_idx].size(); ++idx) {
					const uint64_t& term_idx = high_df_term_idxs[col_idx][idx];
					BloomEntry& bloom_entry = bloom_entries[col_idx][idx];

					float df = IP.II[col_idx].doc_freqs[term_idx];
					float idf = log((IP.num_docs - df + 0.5) / (df + 0.5));

					for (auto& [doc_id, score] : doc_scores) {

						if (bloom_entry.bloom_filter.query((uint64_t)doc_id)) {
							score += _compute_bm25(
									doc_id, 
									1.0f,
									idf, 
									partition_id
									) * boost_factors[col_idx];
						}
					}
				}
			}
		}
	}

	if (DEBUG) {
		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double, std::milli> elapsed_ms = end - start;
		std::cout << "Number of docs: " << doc_scores.size() << "   GATHER TIME: ";
		std::cout << elapsed_ms.count() << "ms" << std::endl;
		fflush(stdout);
	}
	
	if (doc_scores.size() == 0) {
		return std::vector<BM25Result>();
	}

	start = std::chrono::high_resolution_clock::now();
	std::priority_queue<
		BM25Result,
		std::vector<BM25Result>,
		_compare_bm25_result> top_k_docs;

	for (const auto& pair : doc_scores) {
		BM25Result result {
			.doc_id = pair.first,
			.score  = pair.second,
			.partition_id = partition_id
		};
		top_k_docs.push(result);
		if (top_k_docs.size() > k) {
			top_k_docs.pop();
		}
	}

	std::vector<BM25Result> result(top_k_docs.size());
	int idx = top_k_docs.size() - 1;
	while (!top_k_docs.empty()) {
		result[idx] = top_k_docs.top();
		top_k_docs.pop();
		--idx;
	}

	return result;
}


/*
static inline uint64_t get_doc_id(
		InvertedIndexElement* IIE,
		uint32_t& current_idx,
		uint64_t& prev_doc_id
		) {
	uint64_t doc_id;

	current_idx += decompress_uint64_differential_single_bytes(
			&(IIE->doc_ids[current_idx]),
			doc_id,
			prev_doc_id
			);

	prev_doc_id = doc_id;

	return doc_id;
}

static inline std::pair<uint64_t, uint16_t> pop_replace_minheap(
		std::priority_queue<
			std::pair<uint64_t, uint16_t>,
			std::vector<std::pair<uint64_t, uint16_t>>,
			_compare_64_16>& min_heap,
		InvertedIndexElement** II_streams,
		std::vector<uint32_t>& stream_idxs,
		std::vector<uint64_t>& prev_doc_ids
		) {

	std::pair<uint64_t, uint16_t> min = min_heap.top(); min_heap.pop();
	uint16_t min_idx = min.second;

	uint64_t doc_id = get_doc_id(II_streams[min_idx], stream_idxs[min_idx], prev_doc_ids[min_idx]);
	if (stream_idxs[min_idx] < II_streams[min_idx]->doc_ids.size() - 1) {
		min_heap.push(std::make_pair(doc_id, min_idx));
	}

	return std::make_pair(min.first, min_idx);
}



std::vector<BM25Result> _BM25::_query_partition_streaming(
		std::string& query, 
		uint32_t k,
		uint32_t query_max_df,
		uint16_t partition_id,
		std::vector<float> boost_factors
		) {
	std::vector<std::vector<uint64_t>> term_idxs(search_cols.size());
	BM25Partition& IP = index_partitions[partition_id];

	uint64_t doc_offset = (file_type == IN_MEMORY) ? partition_boundaries[partition_id] : 0;

	std::string substr = "";
	for (const char& c : query) {
		if (c != ' ') {
			substr += toupper(c); 
			continue;
		}

		add_query_term(substr, term_idxs, partition_id);	
	}
	if (!substr.empty()) {
		add_query_term(substr, term_idxs, partition_id);
	}

	uint32_t total_terms = 0;
	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		total_terms += term_idxs[col_idx].size();
	}
	if (total_terms == 0) return std::vector<BM25Result>();

	InvertedIndexElement** II_streams = (InvertedIndexElement**)malloc(total_terms * sizeof(InvertedIndexElement*));

	std::vector<uint64_t> doc_freqs(total_terms, 0);
	std::vector<float> idfs(total_terms, 0);
	uint32_t cntr = 0;
	for (uint16_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
		for (const uint64_t& term_idx : term_idxs[col_idx]) {
			doc_freqs[cntr] = get_rle_u8_row_size(
					IP.II[col_idx].inverted_index_compressed[term_idx].term_freqs
					);
			idfs[cntr] = log((IP.num_docs - doc_freqs[cntr] + 0.5) / (doc_freqs[cntr] + 0.5));
			II_streams[cntr++] = &IP.II[col_idx].inverted_index_compressed[term_idx];
		}
	}

	std::priority_queue<
		BM25Result,
		std::vector<BM25Result>,
		_compare_bm25_result> top_k_docs;

	// Define minheap of size total_terms to keep track of current smallest doc_id and its index
	std::priority_queue<
		std::pair<uint64_t, uint16_t>,
		std::vector<std::pair<uint64_t, uint16_t>>,
		_compare_64_16> min_heap;

	std::vector<uint64_t> prev_doc_ids(total_terms, 0);
	std::vector<uint32_t> stream_idxs(total_terms, 0);
	std::vector<std::pair<uint16_t, uint32_t>> tf_counters(total_terms, std::make_pair(0, 0));
	std::vector<uint32_t> tf_idxs(total_terms, 0);

	// Initialize min_heap with first doc_id from each term
	for (uint32_t i = 0; i < total_terms; ++i) {
		if (doc_freqs[i] == 0 || doc_freqs[i] > query_max_df || doc_freqs[i] > max_df) {
			continue;
		}

		uint64_t doc_id = get_doc_id(II_streams[i], stream_idxs[i], prev_doc_ids[i]);
		min_heap.push(std::make_pair(doc_id, i));

		// Initialize tf_counters
		tf_counters[i].first  = II_streams[i]->term_freqs[0].value;
		tf_counters[i].second = II_streams[i]->term_freqs[0].num_repeats;
		tf_idxs[i] = 1;
	}

	BM25Result current_doc;

	uint64_t global_prev_doc_id = 0;
	while (1) {
		std::pair<uint64_t, uint16_t> doc_id_idx_pair = pop_replace_minheap(min_heap, II_streams, stream_idxs, prev_doc_ids);
		if (min_heap.size() == 0) {
			break;
		}

		uint64_t doc_id  = doc_id_idx_pair.first;
		uint16_t min_idx = doc_id_idx_pair.second;
		uint16_t col_idx = min_idx / search_cols.size();

		float idf = idfs[min_idx];
		float tf  = tf_counters[min_idx].first;

		// Score
		if (doc_id != global_prev_doc_id) {
			// New doc_id.
			// Add previous doc_id to top_k_docs
			if (current_doc.doc_id != 0) {
				top_k_docs.push(current_doc);
				if (top_k_docs.size() > k) {
					top_k_docs.pop();
				}
			}

			current_doc.doc_id = doc_id + doc_offset;
			current_doc.score = _compute_bm25(doc_id, tf, idf, partition_id) * boost_factors[col_idx];
			current_doc.partition_id = partition_id;
		} else {
			// Same doc_id.
			current_doc.score += _compute_bm25(doc_id, tf, idf, partition_id) * boost_factors[col_idx];
			printf("Doc id: %lu\n", doc_id);
			fflush(stdout);
			printf("Min idx: %u\n", min_idx);
			fflush(stdout);
			printf("Col idx: %u\n", col_idx);
			fflush(stdout);
			printf("IDF: %f\n", idf);
			fflush(stdout);
			printf("TF: %f\n", tf);
			fflush(stdout);
			printf("Same doc_id: %lu\n", doc_id);
			printf("Score: %f\n\n", current_doc.score);
			fflush(stdout);
		}

		--tf_counters[min_idx].second;
		if (tf_counters[min_idx].second == 0) {
			// Get next RLE pair.
			if (tf_idxs[min_idx] < doc_freqs[min_idx]) {
				++tf_idxs[min_idx];
				tf_counters[min_idx].first  = II_streams[min_idx]->term_freqs[tf_idxs[min_idx]].value;
				tf_counters[min_idx].second = II_streams[min_idx]->term_freqs[tf_idxs[min_idx]].num_repeats;
			}
		}

		global_prev_doc_id = doc_id;
	}

	std::vector<BM25Result> result(top_k_docs.size());
	int idx = top_k_docs.size() - 1;
	while (!top_k_docs.empty()) {
		result[idx] = top_k_docs.top();
		top_k_docs.pop();
		--idx;
	}

	free(II_streams);

	return result;
}
*/


std::vector<BM25Result> _BM25::query(
		std::string& query, 
		uint32_t k,
		uint32_t query_max_df,
		std::vector<float> boost_factors
		) {
	auto start = std::chrono::high_resolution_clock::now();

	if (boost_factors.size() == 0) {
		boost_factors.resize(search_cols.size());
		memset(
				&boost_factors[0],
				1.0f,
				search_cols.size() * sizeof(float)
			  );
	}

	if (boost_factors.size() != search_cols.size()) {
		std::cout << "Error: Boost factors must be the same size as the number of search fields." << std::endl;
		std::cout << "Number of search fields: " << search_cols.size() << std::endl;
		std::cout << "Number of boost factors: " << boost_factors.size() << std::endl;
		std::exit(1);
	}

	std::vector<std::thread> threads;
	std::vector<std::vector<BM25Result>> results(num_partitions);

	// _query_partition on each thread
	for (uint16_t i = 0; i < num_partitions; ++i) {
		threads.push_back(std::thread(
			[this, &query, k, query_max_df, i, &results, boost_factors] {
				// results[i] = _query_partition(query, k, query_max_df, i, boost_factors);
				// results[i] = _query_partition_streaming(query, k, query_max_df, i, boost_factors);
				results[i] = _query_partition_bloom(query, k, query_max_df, i, boost_factors);
			}
		));
	}

	for (auto& thread : threads) {
		thread.join();
	}

	if (results.size() == 0) {
		return std::vector<BM25Result>();
	}

	uint64_t total_matching_docs = 0;

	// Join results. Keep global max heap of size k
	std::priority_queue<
		BM25Result,
		std::vector<BM25Result>,
		_compare_bm25_result> top_k_docs;

	for (const auto& partition_results : results) {
		total_matching_docs += partition_results.size();
		for (const auto& pair : partition_results) {
			top_k_docs.push(pair);
			if (top_k_docs.size() > k) {
				top_k_docs.pop();
			}
		}
	}
	
	std::vector<BM25Result> result(top_k_docs.size());
	int idx = top_k_docs.size() - 1;
	while (!top_k_docs.empty()) {
		result[idx] = top_k_docs.top();
		top_k_docs.pop();
		--idx;
	}

	if (DEBUG) {
		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double, std::milli> elapsed_ms = end - start;
		std::cout << "QUERY: " << query << std::endl;
		std::cout << "Total matching docs: " << total_matching_docs << "    ";
		std::cout << elapsed_ms.count() << "ms" << std::endl;
		fflush(stdout);
	}

	return result;
}

std::vector<std::vector<std::pair<std::string, std::string>>> _BM25::get_topk_internal(
		std::string& _query,
		uint32_t top_k,
		uint32_t query_max_df,
		std::vector<float> boost_factors
		) {

	std::vector<std::vector<std::pair<std::string, std::string>>> result;
	std::vector<BM25Result> top_k_docs = query(_query, top_k, query_max_df, boost_factors);
	result.reserve(top_k_docs.size());

	std::vector<std::pair<std::string, std::string>> row;
	for (size_t i = 0; i < top_k_docs.size(); ++i) {
		switch (file_type) {
			case CSV:
				row = get_csv_line(top_k_docs[i].doc_id, top_k_docs[i].partition_id);
				break;
			case JSON:
				row = get_json_line(top_k_docs[i].doc_id, top_k_docs[i].partition_id);
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
		row.push_back(std::make_pair("score", std::to_string(top_k_docs[i].score)));
		result.push_back(row);
	}
	return result;
}
