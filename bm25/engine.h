#include <iostream>
#include <vector>
#include <string>
#include <cstdint>

#include "robin_hood.h"
#include "vbyte_encoding.h"


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

enum SupportedFileTypes {
	CSV,
	JSON,
	IN_MEMORY
};

typedef struct {
	std::vector<uint8_t> doc_ids;
	std::vector<uint8_t> term_freqs;

} InvertedIndexElement;

typedef struct {
	std::vector<uint64_t> prev_doc_ids;
	std::vector<InvertedIndexElement> inverted_index_compressed;
} InvertedIndex;

inline std::vector<uint64_t> get_II_row(
		InvertedIndex* II, 
		uint64_t term_idx
		);

void serialize_vector_u8(const std::vector<uint8_t>& vec, const std::string& filename);
void serialize_vector_u32(const std::vector<uint32_t>& vec, const std::string& filename);
void serialize_vector_u64(const std::vector<uint64_t>& vec, const std::string& filename);
void serialize_inverted_index(const InvertedIndex& II, const std::string& filename);
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
void serialize_vector_of_vectors_u8(
		const std::vector<std::vector<uint8_t>>& vec, 
		const std::string& filename
		);


void deserialize_vector_u8(std::vector<uint8_t>& vec, const std::string& filename);
void deserialize_vector_u32(std::vector<uint32_t>& vec, const std::string& filename);
void deserialize_vector_u64(std::vector<uint64_t>& vec, const std::string& filename);
void deserialize_inverted_index(InvertedIndex& II, const std::string& filename);
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
void deserialize_vector_of_vector_u8(
		std::vector<std::vector<uint8_t>>& vec, 
		const std::string& filename
		);

class _BM25 {
	public:
		InvertedIndex II;
		robin_hood::unordered_flat_map<std::string, uint64_t> unique_term_mapping;
		std::vector<uint64_t> doc_sizes;
		std::vector<uint64_t> line_offsets;
		robin_hood::unordered_flat_set<std::string> stop_words;

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

		FILE* reference_file;

		_BM25(
				std::string filename,
				std::string search_col,
				int   min_df,
				float max_df,
				float k1,
				float b,
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
			reference_file = fopen(filename.c_str(), "r");
		}

		_BM25(
				std::vector<std::string>& documents,
				int   min_df,
				float max_df,
				float k1,
				float b,
				const std::vector<std::string>& _stop_words = {}
				);

		~_BM25() {
			if (reference_file != nullptr) {
				fclose(reference_file);
			}
		}

		void save_to_disk(const std::string& db_dir);
		void load_from_disk(const std::string& db_dir);

		void process_doc(
				const char* doc,
				const char terminator,
				uint64_t doc_id,
				uint64_t& unique_terms_found
				);

		void read_json();
		void read_csv();
		std::vector<std::pair<std::string, std::string>> get_csv_line(int line_num);
		std::vector<std::pair<std::string, std::string>> get_json_line(int line_num);

		void init_dbs();

		void write_row_to_inverted_index_db(
				const std::string& term,
				uint64_t doc_id
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

		std::vector<std::vector<std::pair<std::string, std::string>>> get_topk_internal(
				std::string& _query,
				uint32_t top_k,
				uint32_t init_max_df
				);
};
