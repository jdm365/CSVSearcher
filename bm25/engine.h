#include <vector>
#include <string>
#include <cstdint>

#include "robin_hood.h"

static const std::string DIR_NAME      			  = "bm25_db";
static const std::string UNIQUE_TERM_MAPPING_PATH = DIR_NAME + "/" + "DOC_TERM_FREQS.bin";
static const std::string DOC_TERM_FREQS_PATH      = DIR_NAME + "/" + "DOC_TERM_FREQS.bin";
static const std::string INVERTED_INDEX_PATH      = DIR_NAME + "/" + "INVERTED_INDEX.bin";
static const std::string TERM_FREQS_FILE_PATH     = DIR_NAME + "/" + "TERM_FREQS.bin";
static const std::string CSV_LINE_OFFSETS_PATH    = DIR_NAME + "/" + "CSV_LINE_OFFSETS.bin";
static const std::string METADATA_PATH			  = DIR_NAME + "/" + "METADATA.bin";

#define DEBUG 1

struct _compare {
	inline bool operator()(const std::pair<uint32_t, float>& a, const std::pair<uint32_t, float>& b) {
		return a.second > b.second;
	}
};

enum SupportedFileTypes {
	CSV,
	JSON	
};

void serialize_vector_u32(const std::vector<uint32_t>& vec, const std::string& filename);
void serialize_vector_u64(const std::vector<uint64_t>& vec, const std::string& filename);
void serialize_vector_of_vectors_u32(const std::vector<std::vector<uint32_t>>& vec, const std::string& filename);
void serialize_robin_hood_flat_map_string_u32(
		const robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		);
void serialize_vector_of_vectors_pair_u32_u16(
		const std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		);


void deserialize_vector_u32(std::vector<uint32_t>& vec, const std::string& filename);
void deserialize_vector_u64(std::vector<uint64_t>& vec, const std::string& filename);
void deserialize_vector_of_vectors_u32(std::vector<std::vector<uint32_t>>& vec, const std::string& filename);
void deserialize_robin_hood_flat_map_string_u32(
		robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		);
void deserialize_vector_of_vectors_pair_u32_u16(
		std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		);

class _BM25 {
	public:
		robin_hood::unordered_flat_map<std::string, uint32_t> unique_term_mapping;

		std::vector<std::vector<uint32_t>> inverted_index;
		std::vector<std::vector<std::pair<uint32_t, uint16_t>>> term_freqs;
		std::vector<uint32_t> doc_term_freqs;
		std::vector<uint32_t> doc_sizes;

		std::vector<uint64_t> line_offsets;

		uint32_t num_docs;
		int      min_df;
		float    avg_doc_size;
		float    max_df;
		float    k1;
		float    b;
		bool     cache_term_freqs;
		bool     cache_inverted_index;
		bool     cache_doc_term_freqs;

		SupportedFileTypes file_type;

		std::string filename;
		std::vector<std::string> columns;
		std::string search_col;

		_BM25(
				std::string filename,
				std::string search_col,
				int   min_df,
				float max_df,
				float k1,
				float b,
				bool cache_term_freqs,
				bool cache_inverted_index,
				bool cache_doc_term_freqs
				);

		~_BM25() {}

		void save_to_disk();

		void read_json(std::vector<uint32_t>& terms);
		void read_csv(std::vector<uint32_t>& terms);
		std::vector<std::pair<std::string, std::string>> get_csv_line(int line_num);
		std::vector<std::pair<std::string, std::string>> get_json_line(int line_num);

		void init_dbs();

		void write_row_to_inverted_index_db(
				const std::string& term,
				uint32_t doc_id
				);
		uint32_t get_doc_term_freq_db(
				const uint32_t& term
				);
		std::vector<uint32_t> get_inverted_index_db(
				const uint32_t& term_idx
				);

		void write_term_freqs_to_file();
		uint16_t get_term_freq_from_file(
				int line_num,
				const uint32_t& term_idx
				);

		float _compute_idf(
				const uint32_t& term
				);
		float _compute_bm25(
				const uint32_t& term,
				uint32_t doc_id,
				float tf,
				float idf
				);

		std::vector<std::pair<uint32_t, float>> query(
				std::string& query,
				uint32_t top_k,
				uint32_t init_max_df
				);

		std::vector<std::vector<std::pair<std::string, std::string>>> get_topk_internal(
				std::string& _query,
				uint32_t top_k,
				uint32_t init_max_df
				);
};
