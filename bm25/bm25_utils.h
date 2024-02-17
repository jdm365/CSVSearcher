#include <vector>
#include <string>
#include <unordered_map>
#include <queue>
#include <cstdint>

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
	std::unordered_map<std::string, std::vector<uint32_t>>& inverted_index,
	std::vector<std::unordered_map<std::string, uint32_t>>& term_freqs,
	std::unordered_map<std::string, uint32_t>& doc_term_freqs,
	std::vector<uint16_t>& doc_sizes,
	float& avg_doc_size,
	uint32_t& num_docs,
	int min_df,
	float max_df
	);

typedef std::pair<uint32_t, float> score_pair_t;
typedef std::priority_queue<score_pair_t, std::vector<score_pair_t>, std::greater<score_pair_t>> min_heap_t;

class _BM25 {
	public:

		std::unordered_map<std::string, std::vector<uint32_t>> inverted_index;
		std::vector<std::unordered_map<std::string, uint32_t>> term_freqs;
		std::unordered_map<std::string, uint32_t> doc_term_freqs;
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
				uint32_t doc_id
				);


		std::vector<std::pair<uint32_t, float>> query(
				std::string& query,
				uint32_t top_k
				);
};
