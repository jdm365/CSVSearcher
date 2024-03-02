#include <vector>
#include <string>
#include <cstdint>

#include "robin_hood.h"

static const std::string DIR_NAME      			= "bm25_db";
static const std::string DOC_TERM_FREQS_DB_NAME = "DOC_TERM_FREQS";
static const std::string INVERTED_INDEX_DB_NAME = "INVERTED_INDEX";
static const std::string TERM_FREQS_FILE_NAME   = "TERM_FREQS";
static const std::string CSV_LINE_OFFSETS_NAME  = "CSV_LINE_OFFSETS";
static const std::string MISC  					= "MISC";

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

class _BM25 {
	public:
		robin_hood::unordered_flat_map<std::string, uint32_t> unique_term_mapping;

		std::vector<std::vector<uint32_t>> inverted_index;
		std::vector<std::vector<std::pair<uint32_t, uint16_t>>> term_freqs;
		// std::vector<robin_hood::unordered_flat_map<uint32_t, uint16_t>> term_freqs;
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

		// MMaped vector
		std::vector<uint32_t> term_freq_line_offsets;

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
