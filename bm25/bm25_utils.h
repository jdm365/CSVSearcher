#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
