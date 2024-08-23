#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <mutex>

#include <parallel_hashmap/phmap.h>
#include <parallel_hashmap/btree.h>
// #include "robin_hood.h"

#include "bloom.h"

#define MAP phmap::flat_hash_map
// #define MAP phmap::btree_map
// #define SET robin_hood::unordered_flat_set
#define SET phmap::flat_hash_set

#define DEBUG 0

#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))

#define SEED 42
#define TOKEN_STREAM_CAPACITY 1'048'576


enum SupportedFileTypes {
	CSV,
	JSON,
	IN_MEMORY
};

enum TermType {
	UNKNOWN,
	LOW_DF,
	HIGH_DF
};


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

struct _compare_64_16 {
	inline bool operator()(const std::pair<uint64_t, uint16_t>& a, const std::pair<uint64_t, uint16_t>& b) {
		return a.second > b.second;
	}
};


////////////////////////////////////////
//////////////// NEW ///////////////////
////////////////////////////////////////

typedef struct {
	uint32_t* term_ids;
	uint8_t*  term_freqs;
	FILE* 	  file;
	uint32_t  num_terms;
} TokenStream;

void init_token_stream(TokenStream* token_stream, const std::string& filename);
void add_token(
		TokenStream* token_stream,
		uint32_t term_id,
		uint8_t term_freq,
		bool new_doc
		);
void free_token_stream(TokenStream* token_stream);
void flush_token_stream(TokenStream* token_stream);


// First 8 bits - tf     (u8)
// Next 24 bits - doc_id (u24)
typedef struct {
	// uint32_t tf : 8;
	// uint32_t doc_id : 24;
	uint32_t tf : 4;
	uint32_t doc_id : 28;
} tf_df_t;

typedef struct {
	tf_df_t*  doc_ids;
	uint16_t* doc_sizes;
	uint32_t* doc_offsets;
	uint32_t* doc_freqs;

	uint32_t  num_terms;
	uint32_t  num_docs;
	float     avg_doc_size;
} InvertedIndexNew;

void init_inverted_index_new(InvertedIndexNew* II);
void read_token_stream(
		InvertedIndexNew* II,
		TokenStream* token_stream
		);
void free_inverted_index_new(InvertedIndexNew* II);

typedef struct {
	InvertedIndexNew* II;
	MAP<std::string, uint32_t>* unique_term_mappings;
	uint64_t* line_offsets;

	uint64_t num_docs;
} BM25PartitionNew;

void init_bm25_partition_new(BM25PartitionNew* IP, uint64_t num_docs, uint16_t num_cols);
void free_bm25_partition_new(BM25PartitionNew* IP);

////////////////////////////////////////


typedef struct {
	uint32_t df;
	std::vector<uint32_t> doc_ids;
	std::vector<uint8_t> term_freqs;
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



typedef struct {
	uint8_t num_repeats;
	uint8_t value;
} RLEElement_u8;

RLEElement_u8 init_rle_element_u8(uint8_t value);
uint64_t get_rle_element_u8_size(const RLEElement_u8& rle_element);
bool check_rle_u8_row_size(const std::vector<RLEElement_u8>& rle_row, uint64_t max_size);
void add_rle_element_u8(std::vector<RLEElement_u8>& rle_row, uint8_t value);


typedef struct {
	MAP<uint16_t, BloomFilter> bloom_filters;
	std::vector<uint64_t> topk_doc_ids;
	std::vector<uint8_t> topk_term_freqs;
} BloomEntry;

BloomEntry init_bloom_entry(
		double fpr, 
		MAP<uint8_t, uint64_t>& tf_map,
		uint64_t min_df_bloom 
		);

typedef struct {
	std::vector<uint8_t> doc_ids;
	std::vector<RLEElement_u8> term_freqs;
} StandardEntry;

typedef struct {
	// Construction artifact
	std::vector<uint64_t> prev_doc_ids;

	// Query time parameters
	MAP<uint64_t, BloomEntry> bloom_filters;
	std::vector<StandardEntry> inverted_index_compressed;
	std::vector<uint32_t> doc_freqs;

	std::vector<uint16_t> doc_sizes;
	float avg_doc_size;

	// EXPERIMENTAL
	TokenStream token_stream;
} InvertedIndex;

inline IIRow get_II_row(InvertedIndex* II, uint64_t term_idx);
inline IIRow get_II_row_new(InvertedIndexNew* II, uint64_t term_idx);

typedef struct {
	std::vector<InvertedIndex> II;
	std::vector<MAP<std::string, uint32_t>> unique_term_mapping;
	std::vector<uint64_t> line_offsets;

	uint64_t num_docs;
} BM25Partition;


class _BM25 {
	public:
		BM25PartitionNew* index_partitions;
		SET<std::string> stop_words;

		uint64_t num_docs;
		float    bloom_df_threshold;
		double   bloom_fpr;
		float    k1;
		float    b;
		uint16_t num_partitions;

		SupportedFileTypes file_type;

		std::vector<std::string> search_cols;
		std::string filename;
		std::vector<std::string> columns;
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
				std::vector<std::string> search_cols,
				float  bloom_df_threshold,
				double bloom_fpr,
				float  k1,
				float  b,
				uint16_t num_partitions,
				const std::vector<std::string>& _stop_words = {}
				);

		_BM25(std::string db_dir) {
			load_from_disk(db_dir);
		}

		_BM25(
				std::vector<std::vector<std::string>>& documents,
				float  bloom_df_threshold,
				double bloom_fpr,
				float  k1,
				float  b,
				uint16_t num_partitions,
				const std::vector<std::string>& _stop_words = {}
				);

		~_BM25() {
			for (auto& handle : reference_file_handles) {
				if (handle != nullptr) {
					fclose(handle);
				}
			}

			for (size_t partition_idx = 0; partition_idx < (size_t)num_partitions; ++partition_idx) {
				/*
				BM25PartitionNew* IP = &index_partitions[partition_idx];

				for (size_t col_idx = 0; col_idx < search_cols.size(); ++col_idx) {
					InvertedIndexNew* II = &IP->II[col_idx];
					for (auto& [doc_id, bloom_entry] : II.bloom_filters) {
						for (auto& [tf, filter] : bloom_entry.bloom_filters) {
							bloom_free(filter);
						}
					}
				}
				*/
				free_bm25_partition_new(&index_partitions[partition_idx]);
			}
			free(index_partitions);
		}
		void init_terminal();
		void proccess_csv_header();

		void save_index_partition(std::string db_dir, uint16_t partition_id);
		void load_index_partition(std::string db_dir, uint16_t partition_id);
		void save_to_disk(const std::string& db_dir);
		void load_from_disk(const std::string& db_dir);

		uint32_t process_doc_partition_json(
				const char* doc,
				const char terminator,
				TokenStream* token_stream,
				uint64_t doc_id,
				uint16_t partition_id,
				uint16_t col_idx,
				uint32_t* doc_freqs_capacity
				);
		uint32_t process_doc_partition_rfc_4180(
				const char* doc,
				const char terminator,
				uint64_t doc_id,
				uint32_t& unique_terms_found,
				uint16_t partition_id,
				uint16_t col_idx
				);

		uint32_t _process_doc_partition_rfc_4180_quoted(
				const char* doc,
				uint64_t doc_id,
				uint32_t& unique_terms_found,
				uint16_t partition_id,
				uint16_t col_idx
				);
		void process_doc_partition_rfc_4180_mmap(
				const char* file_data,
				const char terminator,
				uint64_t doc_id,
				uint32_t& unique_terms_found,
				uint16_t partition_id,
				uint16_t col_idx,
				uint64_t& byte_offset
				);


		uint32_t process_doc_partition_rfc_4180_v2(
				const char* doc,
				const char terminator,
				TokenStream* token_stream,
				uint64_t doc_id,
				uint16_t partition_id,
				uint16_t col_idx,
				uint32_t* doc_freqs_capacity
				);
		uint32_t _process_doc_partition_rfc_4180_quoted_v2(
				const char* doc,
				TokenStream* token_stream,
				uint64_t doc_id,
				uint16_t partition_id,
				uint16_t col_idx,
				uint32_t* doc_freqs_capacity
				);

		void determine_partition_boundaries_csv_rfc_4180();
		void determine_partition_boundaries_json();

		void write_bloom_filters(uint16_t partition_id);
		void read_json(uint64_t start_byte, uint64_t end_byte, uint16_t partition_id);
		void read_csv_rfc_4180(uint64_t start_byte, uint64_t end_byte, uint16_t partition_id);
		void read_csv_rfc_4180_mmap(uint64_t start_byte, uint64_t end_byte, uint16_t partition_id);
		void read_in_memory(
				std::vector<std::vector<std::string>>& documents,
				uint64_t start_idx, 
				uint64_t end_idx, 
				uint16_t partition_id
				);
		std::vector<std::pair<std::string, std::string>> get_csv_line(uint32_t line_num, uint16_t partition_id);
		std::vector<std::pair<std::string, std::string>> get_json_line(uint32_t line_num, uint16_t partition_id);

		void init_dbs();
		uint64_t get_doc_freqs_sum(
				std::string& term,
				uint16_t col_idx
				);
		void write_row_to_inverted_index_db(
				const std::string& term,
				uint64_t doc_id
				);
		float _compute_bm25(
				uint64_t doc_id,
				float tf,
				float idf,
				uint16_t col_idx,
				uint16_t partition_id
				);
		void add_query_term(
				std::string& substr,
				std::vector<std::vector<uint64_t>>& term_idxs,
				uint16_t partition_id
				);
		TermType add_query_term_bloom(
				std::string& substr,
				std::vector<std::vector<uint64_t>>& term_idxs,
				std::vector<MAP<uint64_t, BloomEntry>>& bloom_entries,
				uint16_t partition_id,
				uint16_t col_idx
				);

		std::vector<BM25Result> _query_partition_streaming(
				std::string& query,
				uint32_t top_k,
				uint32_t query_max_df,
				uint16_t partition_id,
				std::vector<float> boost_factors
				);

		std::vector<BM25Result> query(
				std::string& query,
				uint32_t top_k,
				uint32_t query_max_df,
				std::vector<float> boost_factors
				);
		std::vector<std::vector<std::pair<std::string, std::string>>> get_topk_internal(
				std::string& _query,
				uint32_t top_k,
				uint32_t query_max_df,
				std::vector<float> boost_factors
				);

		std::vector<BM25Result> _query_partition_bloom_multi(
				std::vector<std::string>& query,
				uint32_t k,
				uint32_t query_max_df,
				uint16_t partition_id,
				std::vector<float> boost_factors,
				std::vector<std::vector<uint64_t>> doc_freqs
				);
		std::vector<BM25Result> query_multi(
				std::vector<std::string>& query,
				uint32_t top_k,
				uint32_t query_max_df,
				std::vector<float> boost_factors
				);
		std::vector<std::vector<std::pair<std::string, std::string>>> get_topk_internal_multi(
				std::vector<std::string>& query,
				uint32_t top_k,
				uint32_t query_max_df,
				std::vector<float> boost_factors
				);

		void update_progress(int line_num, int num_lines, uint16_t partition_id);
		void finalize_progress_bar();
};
