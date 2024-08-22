#include <stdio.h>
#include <assert.h>

#include <chrono>
#include <vector>
#include <string>

#include "../bm25/engine.h"
#include "../bm25/bloom.h"
#include "../bm25/robin_hood.h"

#ifdef NDEBUG
	#undef NDEBUG
#endif

#define PRINT_DEBUG 1


void test_save_load() {
	std::string FILENAME = "tests/mb_small.csv";
	std::vector<std::string> SEARCH_COLS = {"title", "artist"};

	float BLOOM_DF_THRESHOLD = 0.005f;
	double BLOOM_FPR = 0.000001;
	float K1 = 1.2f;
	float B = 0.75f;
	uint16_t NUM_PARTITIONS = 24;

	_BM25 bm25(
		FILENAME,
		SEARCH_COLS,
		BLOOM_DF_THRESHOLD,
		BLOOM_FPR,
		K1,
		B,
		NUM_PARTITIONS
	);

	/*
	std::string tmp_filename = "tests/bm25_test_dir";
	bm25.save_to_disk(tmp_filename);

	_BM25 bm25_loaded(tmp_filename);

	// Compare values of bm25 and bm25_loaded
	assert(bm25.index_partitions.size() == bm25_loaded.index_partitions.size());
	for (uint16_t i = 0; i < bm25.index_partitions.size(); ++i) {
		BM25Partition& INITIAL = bm25.index_partitions[i];
		BM25Partition& LOADED  = bm25_loaded.index_partitions[i];

		assert(INITIAL.II.size() == LOADED.II.size());
		for (uint16_t j = 0; j < INITIAL.II.size(); ++j) {

			// Check II equivalence
			assert(INITIAL.II[j].bloom_filters.size() == LOADED.II[j].bloom_filters.size());
			for (const auto& [init_doc_id, init_bloom_entry] : INITIAL.II[j].bloom_filters) {
				BloomEntry& loaded_bloom_entry = LOADED.II[j].bloom_filters.at(init_doc_id);

				// Check bloom equivalence
				assert(init_bloom_entry.bloom_filters.size() == loaded_bloom_entry.bloom_filters.size());
				for (const auto& [init_term, init_bloom] : init_bloom_entry.bloom_filters) {
					BloomFilter& loaded_bloom = loaded_bloom_entry.bloom_filters.at(init_term);
					// ChunkedBloomFilter& loaded_bloom = loaded_bloom_entry.bloom_filters.at(init_term);

					// assert(init_bloom.num_filters == loaded_bloom.num_filters);
					// assert(init_bloom.num_elements_chunk == loaded_bloom.num_elements_chunk);
					for (uint64_t filter_idx = 0; filter_idx < init_bloom.num_filters; ++filter_idx) {
						for (uint64_t k = 0; k < init_bloom.num_bits_chunk; ++k) {
							assert(init_bloom.bits[k] == loaded_bloom.bits[k]);
						}
					}
				}


				assert(init_bloom_entry.topk_doc_ids.size() == loaded_bloom_entry.topk_doc_ids.size());
				assert(init_bloom_entry.topk_term_freqs.size() == loaded_bloom_entry.topk_term_freqs.size());
				for (uint32_t k = 0; k < init_bloom_entry.topk_doc_ids.size(); ++k) {
					assert(init_bloom_entry.topk_doc_ids[k] == loaded_bloom_entry.topk_doc_ids[k]);
					assert(init_bloom_entry.topk_term_freqs[k] == loaded_bloom_entry.topk_term_freqs[k]);
				}
			}
		}

		assert(INITIAL.unique_term_mapping.size() == LOADED.unique_term_mapping.size());
		for (uint32_t map_idx = 0; map_idx < INITIAL.unique_term_mapping.size(); ++map_idx) {
			const robin_hood::unordered_flat_map<std::string, uint32_t>& init_map = INITIAL.unique_term_mapping[map_idx];
			const robin_hood::unordered_flat_map<std::string, uint32_t>& loaded_map = LOADED.unique_term_mapping[map_idx];

			for (const auto& [key, value] : init_map) {
				assert(loaded_map.find(key) != loaded_map.end());
			}
		}

		assert(INITIAL.line_offsets.size() == LOADED.line_offsets.size());
		for (uint64_t j = 0; j < INITIAL.line_offsets.size(); ++j) {
			assert(INITIAL.line_offsets[j] == LOADED.line_offsets[j]);
		}
	}

	assert(bm25.num_docs == bm25_loaded.num_docs);
	assert(bm25.bloom_df_threshold == bm25_loaded.bloom_df_threshold);
	assert(bm25.bloom_fpr == bm25_loaded.bloom_fpr);
	assert(bm25.k1 == bm25_loaded.k1);
	assert(bm25.b == bm25_loaded.b);
	assert(bm25.num_partitions == bm25_loaded.num_partitions);

	assert(bm25.file_type == bm25_loaded.file_type);

	assert(bm25.search_cols.size() == bm25_loaded.search_cols.size());
	for (uint16_t i = 0; i < bm25.search_cols.size(); ++i) {
		assert(bm25.search_cols[i] == bm25_loaded.search_cols[i]);
	}
	assert(bm25.filename == bm25_loaded.filename);
	assert(bm25.columns.size() == bm25_loaded.columns.size());
	for (uint16_t i = 0; i < bm25.columns.size(); ++i) {
		assert(bm25.columns[i] == bm25_loaded.columns[i]);
	}
	assert(bm25.search_col_idxs.size() == bm25_loaded.search_col_idxs.size());
	for (uint16_t i = 0; i < bm25.search_col_idxs.size(); ++i) {
		assert(bm25.search_col_idxs[i] == bm25_loaded.search_col_idxs[i]);
	}
	assert(bm25.header_bytes == bm25_loaded.header_bytes);

	assert(bm25.partition_boundaries.size() == bm25_loaded.partition_boundaries.size());
	for (uint16_t i = 0; i < bm25.partition_boundaries.size(); ++i) {
		assert(bm25.partition_boundaries[i] == bm25_loaded.partition_boundaries[i]);
	}

	assert(bm25.reference_file_handles.size() == bm25_loaded.reference_file_handles.size());
	*/
}

void test_multi_query() {
	const std::vector<std::vector<std::string>> MULTI_QUERIES = {
		{"", "UNDER MY SKIN DEEP IN THE HEART OF ME SO DEEP IN MY HEART THAT YOUR REALLY A PART OF ME BOO DOO DOO DOO DOO DOO. I'VE GOT YOU UNDER MY SKIN. EVEN MORE WORDS TRYING TO GET A SEGFAULT MAYBE? PERHAPS? ZOIDBERG"},
		{"DRAGON BALL Z", "GOKU"}

	};
	const std::vector<std::string> EXPECTED_URLS = {
		"DUMMY",
		"https://en.wikipedia.org/wiki?curid=1001666"
	};

	std::string FILENAME = "tests/wiki_articles.csv";
	// std::string FILENAME = "tests/wiki_articles_500k.csv";
	std::vector<std::string> SEARCH_COLS = {"title", "body"};

	float BLOOM_DF_THRESHOLD = 0.005f;
	double BLOOM_FPR = 0.000001;
	float K1 = 1.2f;
	float B = 0.75f;
	uint16_t NUM_PARTITIONS = 24;
	// uint16_t NUM_PARTITIONS = 1;

	uint16_t TOP_K = 5;

	auto start = std::chrono::high_resolution_clock::now();

	_BM25 bm25(
		FILENAME,
		SEARCH_COLS,
		BLOOM_DF_THRESHOLD,
		BLOOM_FPR,
		K1,
		B,
		NUM_PARTITIONS
	);

	auto end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	printf("\nIndexing time: %ld ms\n", duration);

	for (uint32_t idx = 0; idx < MULTI_QUERIES.size(); ++idx) {
		std::vector<std::string> _query = MULTI_QUERIES[idx];

		start = std::chrono::high_resolution_clock::now();
		std::vector<std::vector<std::pair<std::string, std::string>>> results = bm25.get_topk_internal_multi(
				_query,
				TOP_K,
				5000000,
				{2.0f, 1.0f}
				);

		// bool found = false;
		for (const std::vector<std::pair<std::string, std::string>>& result : results) {
			if (PRINT_DEBUG) {
				printf("================================\n");
				for (const std::string& query : _query) {
					printf("Query: %s\n", query.c_str());
				}
				printf("--------------------------------\n");
			}

			for (const std::pair<std::string, std::string>& res : result) {
				if (PRINT_DEBUG) {
					if (res.first == "title") {
						printf("Title: %s\n", res.second.c_str());
						continue;
					} else if (res.first == "body") {
						printf("Body: %s\n", res.second.c_str());
						continue;
					} else if (res.first == "url") {
						printf("URL: %s\n", res.second.c_str());
						continue;
					} else if (res.first == "score") {
						printf("Score: %s\n", res.second.c_str());
						continue;
					}
				}

				// if (res.first == "url" && res.second == EXPECTED_URLS[idx]) {
					// found = true;
				// }
			}
			if (PRINT_DEBUG) {
				printf("--------------------------------\n");
				printf("================================\n");
			}
		}
		if (PRINT_DEBUG) {
			end = std::chrono::high_resolution_clock::now();
			duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
			printf("Total Query time: %ld us\n\n", duration);
			printf("Number of results: %ld\n", results.size());
			printf("--------------------------------\n");
		}

		// assert(found);
	}
	printf("Found expected results\n");
}


void _bloom_test_create_query() {
	double GOAL_FPR 	 = 0.0000001;
	uint64_t NUM_ENTRIES = 50000;
	uint64_t UPPER_BOUND = NUM_ENTRIES * 20;

	robin_hood::unordered_flat_set<uint64_t> keys;
	while (keys.size() < NUM_ENTRIES) {
		keys.insert(rand() % UPPER_BOUND);
	}

	BloomFilter filter = init_bloom_filter(NUM_ENTRIES, GOAL_FPR);
	for (const uint64_t& key : keys) {
		bloom_put(filter, key);
	}

	uint64_t TP = 0;
	uint64_t FP = 0;

	// Query all keys
	for (uint64_t key = 0; key < UPPER_BOUND; ++key) {
		bool query = bloom_query(filter, key);
		if (keys.find(key) != keys.end()) {
			if (query) {
				++TP;
			}
		} else {
			if (query) {
				++FP;
			}
		}
	}

	double TPR = (double)TP / keys.size();
	double FPR = (double)FP / (UPPER_BOUND - keys.size());

	assert(TP == keys.size());
	assert(FPR < GOAL_FPR * 2);

	printf("--------------------------------\n");
	printf("Bloom TPR: %f\n", TPR);
	printf("Bloom FPR: %f\n", FPR);
	printf("--------------------------------\n");
}

void _bloom_test_save_load() {
	std::string tmp_filename = "tests/bloom_test.bin";
	double GOAL_FPR 	 = 0.0000001;
	uint64_t NUM_ENTRIES = 50000;
	uint64_t UPPER_BOUND = NUM_ENTRIES * 20;

	robin_hood::unordered_flat_set<uint64_t> keys;
	while (keys.size() < NUM_ENTRIES) {
		keys.insert(rand() % UPPER_BOUND);
	}

	BloomFilter filter = init_bloom_filter(NUM_ENTRIES, GOAL_FPR);
	for (const uint64_t& key : keys) {
		bloom_put(filter, key);
	}

	bloom_save(filter, tmp_filename.c_str());
	bloom_load(filter, tmp_filename.c_str());

	uint64_t TP = 0;
	uint64_t FP = 0;

	// Query all keys
	for (uint64_t key = 0; key < UPPER_BOUND; ++key) {
		bool query = bloom_query(filter, key);
		if (keys.find(key) != keys.end()) {
			if (query) {
				++TP;
			}
		} else {
			if (query) {
				++FP;
			}
		}
	}

	double FPR = (double)FP / (UPPER_BOUND - keys.size());

	assert(TP == keys.size());
	assert(FPR < GOAL_FPR * 2);
}

void bloom_test() {
	_bloom_test_create_query();
	_bloom_test_save_load();
}


int main() {
	test_multi_query();
	// bloom_test();
	// test_save_load();
	return 0;
}
