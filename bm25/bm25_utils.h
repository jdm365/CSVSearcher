#include <vector>
#include <string>
#include <cstdint>

#include "robin_hood.h"
#include <leveldb/db.h>

static const std::string DIR_NAME      			= "bm25_db";
static const std::string DOC_TERM_FREQS_DB_NAME = "DOC_TERM_FREQS";
static const std::string INVERTED_INDEX_DB_NAME = "INVERTED_INDEX";
static const std::string TERM_FREQS_FILE_NAME   = "TERM_FREQS";

#define INIT_MAX_DF 5000
#define DEBUG 0

std::vector<std::string> tokenize_whitespace(
		const std::string& document
		);
std::vector<std::string> tokenize_ngram(
		std::string& document, 
		int ngram_size
		);
void tokenize_whitespace_batch(
		std::vector<std::string>& documents,
		std::vector<std::vector<std::string>>& tokenized_documents
		);
void tokenize_ngram_batch(
		std::vector<std::string>& documents,
		std::vector<std::vector<std::string>>& tokenized_documents,
		int ngram_size
		);

class _BM25 {
	public:
		std::vector<uint16_t> doc_sizes;
		robin_hood::unordered_flat_set<std::string> large_dfs;

		uint32_t num_docs;
		int      ngram_size;
		int      min_df;
		float    avg_doc_size;
		float    max_df;
		float    k1;
		float    b;
		bool     whitespace_tokenization;

		// LevelDB Management
		// Two databases: one for term frequencies, one for inverted index
		leveldb::DB* doc_term_freqs_db;
		leveldb::DB* inverted_index_db;

		// MMaped vector
		std::vector<int> term_freq_line_offsets;

		_BM25(
				std::vector<std::string>& documents,
				bool whitespace_tokenization,
				int ngram_size,
				int min_df,
				float max_df,
				float k1,
				float b
				);
		~_BM25() {
			delete doc_term_freqs_db;
			delete inverted_index_db;
		}

		void init_dbs();

		void create_doc_term_freqs_db(
				const robin_hood::unordered_map<std::string, uint32_t>& doc_term_freqs
				);
		void create_inverted_index_db(
				const robin_hood::unordered_map<std::string, std::vector<uint32_t>>& inverted_index
				);
		void write_row_to_inverted_index_db(
				const std::string& term,
				uint32_t doc_id
				);
		uint32_t get_doc_term_freq_db(
				const std::string& term
				);
		std::vector<uint32_t> get_inverted_index_db(
				const std::string& term
				);

		void write_term_freqs_to_file(
				// const std::vector<robin_hood::unordered_map<std::string, uint16_t>>& term_freqs
				const std::vector<std::vector<std::pair<std::string, uint16_t>>>& term_freqs
				);
		uint16_t get_term_freq_from_file(
				int line_num,
				const std::string& term
				);

		float _compute_idf(
				const std::string& term
				);
		float _compute_bm25(
				const std::string& term,
				uint32_t doc_id,
				float idf
				);

		std::vector<std::pair<uint32_t, float>> query(
				std::string& query,
				uint32_t top_k
				);
};
