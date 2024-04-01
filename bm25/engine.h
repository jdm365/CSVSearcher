#include <vector>
#include <string>
#include <cstdint>

#include "robin_hood.h"
#include "xxhash64.h"
#include "vbyte_encoding.h"

static const std::string DIR_NAME      			  = "bm25_db";
static const std::string UNIQUE_TERM_MAPPING_PATH = DIR_NAME + "/" + "UNIQUE_TERM_MAPPING.bin";
static const std::string INVERTED_INDEX_PATH      = DIR_NAME + "/" + "INVERTED_INDEX.bin";
static const std::string TERM_FREQS_FILE_PATH     = DIR_NAME + "/" + "TERM_FREQS.bin";
static const std::string DOC_TERM_FREQS_PATH      = DIR_NAME + "/" + "DOC_TERM_FREQS.bin";
static const std::string DOC_SIZES_PATH      	  = DIR_NAME + "/" + "DOC_SIZES.bin";
static const std::string LINE_OFFSETS_PATH    	  = DIR_NAME + "/" + "LINE_OFFSETS.bin";
static const std::string METADATA_PATH			  = DIR_NAME + "/" + "METADATA.bin";

#define DEBUG 1

#define SEED 42

static XXHash64 hasher(SEED);

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

enum SupportedFileTypes {
	CSV,
	JSON,
	IN_MEMORY
};

void serialize_vector_u32(const std::vector<uint32_t>& vec, const std::string& filename);
void serialize_vector_u64(const std::vector<uint64_t>& vec, const std::string& filename);
void serialize_vector_of_vectors_u32(
		const std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		);
void serialize_vector_of_vectors_u64(
		const std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		);
void serialize_robin_hood_flat_map_string_u32(
		const robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		);
void serialize_robin_hood_flat_map_string_u64(
		const robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		);
void serialize_vector_of_vectors_pair_u32_u16(
		const std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		);
void serialize_vector_of_vectors_pair_u64_u16(
		const std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		);


void deserialize_vector_u32(std::vector<uint32_t>& vec, const std::string& filename);
void deserialize_vector_u64(std::vector<uint64_t>& vec, const std::string& filename);
void deserialize_vector_of_vectors_u32(
		std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		);
void deserialize_vector_of_vectors_u64(
		std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		);
void deserialize_robin_hood_flat_map_string_u32(
		robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		);
void deserialize_robin_hood_flat_map_string_u64(
		robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		);
void deserialize_vector_of_vectors_pair_u32_u16(
		std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		);
void deserialize_vector_of_vectors_pair_u64_u16(
		std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		);

typedef struct {
	// First element of inverted index is doc_freq.
	// Then elements are doc_ids followed by term_freqs.

	std::vector<robin_hood::unordered_flat_map<uint64_t, uint64_t>> accumulator;
	// std::vector<std::vector<std::pair<uint64_t, uint64_t>>> accumulator;
	std::vector<uint64_t> doc_term_freqs_accumulator;

	std::vector<std::vector<uint8_t>> inverted_index_compressed;

} InvertedIndex;

inline std::vector<uint64_t> get_II_row(
		InvertedIndex* II, 
		uint64_t term_idx
		);

class _BM25 {
	public:
		robin_hood::unordered_flat_map<std::string, uint64_t> unique_term_mapping;
		std::vector<std::vector<uint64_t>> inverted_index;
		std::vector<std::vector<std::pair<uint64_t, uint16_t>>> term_freqs;
		std::vector<uint64_t> doc_term_freqs;
		std::vector<uint64_t> doc_sizes;
		std::vector<uint64_t> line_offsets;

		InvertedIndex II;

		uint64_t num_docs;
		int      min_df;
		float    avg_doc_size;
		float    max_df;
		float    k1;
		float    b;

		SupportedFileTypes file_type;

		std::string search_col;
		std::string filename;
		std::vector<std::string> columns;

		_BM25(
				std::string filename,
				std::string search_col,
				int   min_df,
				float max_df,
				float k1,
				float b
				);
		_BM25(std::string db_dir) {
			load_from_disk(db_dir);
		}

		_BM25(
				std::vector<std::string>& documents,
				int   min_df,
				float max_df,
				float k1,
				float b
				);

		~_BM25() {}

		void save_to_disk();
		void load_from_disk(const std::string& db_dir);

		void read_json(std::vector<uint64_t>& terms);
		void read_csv(std::vector<uint64_t>& terms);
		void read_csv_new();
		std::vector<std::pair<std::string, std::string>> get_csv_line(int line_num);
		std::vector<std::pair<std::string, std::string>> get_json_line(int line_num);

		void init_dbs();

		void write_row_to_inverted_index_db(
				const std::string& term,
				uint64_t doc_id
				);
		uint64_t get_doc_term_freq_db(
				const uint64_t& term
				);
		std::vector<uint64_t> get_inverted_index_db(
				const uint64_t& term_idx
				);

		void write_term_freqs_to_file();
		uint16_t get_term_freq_from_file(
				int line_num,
				const uint64_t& term_idx
				);

		float _compute_idf(
				const uint64_t& term_idx
				);
		float _compute_bm25(
				uint64_t doc_id,
				float tf,
				float idf
				);

		std::vector<std::pair<uint64_t, float>> query(
				std::string& query,
				uint32_t top_k,
				uint32_t init_max_df
				);
		std::vector<std::pair<uint64_t, float>> query_new(
				std::string& query,
				uint32_t top_k,
				uint32_t init_max_df
				);

		std::vector<std::vector<std::pair<std::string, std::string>>> get_topk_internal(
				std::string& _query,
				uint32_t top_k,
				uint32_t init_max_df
				);

		std::vector<std::vector<std::pair<std::string, std::string>>> get_topk_internal(
				std::string& _query,
				uint32_t top_k,
				uint32_t init_max_df,
				uint32_t* mask_idxs,
				uint32_t mask_len
				);
};
