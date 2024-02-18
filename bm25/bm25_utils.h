#include <vector>
#include <string>
#include <cstdint>

#include "robin_hood.h"

static const std::string DB_NAME = "term_freqs.mdb";
static const std::string INVERTED_INDEX_DB_NAME = "INVERTED_INDEX";

std::vector<std::string> tokenize_whitespace(
		std::string& document
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
void init_members(
	std::vector<std::vector<std::string>>& tokenized_documents,
	// robin_hood::unordered_flat_map<std::string, std::vector<uint32_t>>& inverted_index,
	robin_hood::unordered_flat_set<std::string>& large_dfs,
	std::vector<uint16_t>& doc_sizes,
	float& avg_doc_size,
	uint32_t& num_docs,
	int min_df,
	float max_df
	);

class _BM25 {
	public:
		robin_hood::unordered_flat_set<std::string> large_dfs;
		std::vector<uint16_t> doc_sizes;

		float avg_doc_size;
		uint32_t num_docs;
		bool whitespace_tokenization;
		int ngram_size;
		int min_df;
		float max_df;

		float k1;
		float b;

		_BM25(
				std::vector<std::string>& documents,
				bool whitespace_tokenization,
				int ngram_size,
				int min_df,
				float max_df,
				float k1,
				float b
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
