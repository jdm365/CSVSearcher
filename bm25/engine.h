#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <mutex>

#include "robin_hood.h"


#define DEBUG 0

#define SEED 42

struct _compare {
	inline bool operator()(const std::pair<uint32_t, float>& a, const std::pair<uint32_t, float>& b) {
		return a.second > b.second;
	}
};

struct _compare_64 {
	inline bool operator()(const std::pair<uint64_t, float>& a, const std::pair<uint64_t, float>& b) {
		return a.second > b.second;
	}
};

typedef struct {
	uint64_t df;
	std::vector<uint64_t> doc_ids;
	std::vector<float> term_freqs;
} IIRow;

typedef struct {
	uint64_t doc_id;
	float    score;
	uint16_t partition_id;
} BM25Result;

struct _compare_bm25_result {
	inline bool operator()(const BM25Result& a, const BM25Result& b) {
		return a.score > b.score;
	}
};

enum SupportedFileTypes {
	CSV,
	JSON,
	IN_MEMORY
};

uint64_t count_newlines_simd(FILE* f);

typedef struct {
	uint16_t num_repeats;
	uint8_t  value;
} RLEElement_u8;

RLEElement_u8 init_rle_element_u8(uint8_t value);
uint64_t get_rle_element_u8_size(const RLEElement_u8& rle_element);
bool check_rle_u8_row_size(const std::vector<RLEElement_u8>& rle_row, uint64_t max_size);
void add_rle_element_u8(std::vector<RLEElement_u8>& rle_row, uint8_t value);


typedef struct {
	std::vector<uint8_t> doc_ids;
	std::vector<RLEElement_u8> term_freqs;
} InvertedIndexElement;

typedef struct {
	std::vector<uint64_t> prev_doc_ids;
	std::vector<InvertedIndexElement> inverted_index_compressed;
} InvertedIndex;

inline IIRow get_II_row(
		InvertedIndex* II, 
		uint64_t term_idx,
		uint32_t k
		);

typedef struct {
	InvertedIndex II;
	robin_hood::unordered_flat_map<std::string, uint32_t> unique_term_mapping;
	std::vector<uint16_t> doc_sizes;
	std::vector<uint64_t> line_offsets;

	uint64_t num_docs;
	float    avg_doc_size;
} BM25Partition;

BM25Partition init_bm25_partition();


class _BM25 {
	public:
		std::vector<BM25Partition> index_partitions;
		robin_hood::unordered_flat_set<std::string> stop_words;

		uint64_t num_docs;
		int      min_df;
		float    max_df;
		float    k1;
		float    b;
		uint16_t num_partitions;

		SupportedFileTypes file_type;

		// std::string search_col;
		std::vector<std::string> search_cols;
		std::string filename;
		std::vector<std::string> columns;
		// int16_t search_col_idx;
		std::vector<int16_t> search_col_idxs;
		uint16_t header_bytes;

		std::vector<uint64_t> partition_boundaries;

		std::vector<FILE*> reference_file_handles;

		std::vector<std::string> progress_bars;
		std::mutex progress_mutex;
		int init_cursor_row;
		int terminal_height;

		_BM25(
				std::string filename,
				// std::string search_col,
				std::vector<std::string> search_cols,
				int   min_df,
				float max_df,
				float k1,
				float b,
				uint16_t num_partitions,
				const std::vector<std::string>& _stop_words = {}
				);

		_BM25(std::string db_dir) {
			load_from_disk(db_dir);

			// filename found in db_dir/filename.txt
			std::string fn_file = db_dir + "/filename.txt";

			// read fn_file contents into filename 
			FILE* f = fopen(fn_file.c_str(), "r");
			if (f == nullptr) {
				std::cerr << "Error opening file: " << fn_file << std::endl;
				exit(1);
			}
			char buf[1024];
			fgets(buf, 1024, f);
			fclose(f);
			filename = std::string(buf);

			// Open the reference file
			for (uint16_t i = 0; i < num_partitions; i++) {
				FILE* ref_f = fopen(filename.c_str(), "r");
				if (ref_f == nullptr) {
					std::cerr << "Error opening file: " << filename << std::endl;
					exit(1);
				}
				reference_file_handles.push_back(ref_f);
			}
		}

		_BM25(
				std::vector<std::string>& documents,
				int   min_df,
				float max_df,
				float k1,
				float b,
				uint16_t num_partitions,
				const std::vector<std::string>& _stop_words = {}
				);

		~_BM25() {
			for (uint16_t i = 0; i < num_partitions; i++) {
				if (reference_file_handles[i] != nullptr) {
					fclose(reference_file_handles[i]);
				}
			}
		}
		void proccess_csv_header();

		void save_index_partition(std::string db_dir, uint16_t partition_id);
		void load_index_partition(std::string db_dir, uint16_t partition_id);
		void save_to_disk(const std::string& db_dir);
		void load_from_disk(const std::string& db_dir);

		uint32_t process_doc_partition(
				const char* doc,
				const char terminator,
				uint64_t doc_id,
				uint32_t& unique_terms_found,
				uint16_t partition_id
				);

		void determine_partition_boundaries_csv();
		void determine_partition_boundaries_json();

		void read_json(uint64_t start_byte, uint64_t end_byte, uint16_t partition_id);
		void read_csv(uint64_t start_byte, uint64_t end_byte, uint16_t partition_id);
		void read_in_memory(
				std::vector<std::string>& documents,
				uint64_t start_idx, 
				uint64_t end_idx, 
				uint16_t partition_id
				);
		std::vector<std::pair<std::string, std::string>> get_csv_line(int line_num, uint16_t partition_id);
		std::vector<std::pair<std::string, std::string>> get_json_line(int line_num, uint16_t partition_id);

		void init_dbs();

		void write_row_to_inverted_index_db(
				const std::string& term,
				uint64_t doc_id
				);

		float _compute_bm25(
				uint64_t doc_id,
				float tf,
				float idf,
				uint16_t partition_id
				);

		std::vector<BM25Result> query(
				std::string& query,
				uint32_t top_k,
				uint32_t query_max_df
				);
		std::vector<BM25Result> _query_partition(
				std::string& query,
				uint32_t top_k,
				uint32_t query_max_df,
				uint16_t partition_id
				);

		std::vector<std::vector<std::pair<std::string, std::string>>> get_topk_internal(
				std::string& _query,
				uint32_t top_k,
				uint32_t query_max_df
				);

		void update_progress(int line_num, int num_lines, uint16_t partition_id);
		void finalize_progress_bar();
};
