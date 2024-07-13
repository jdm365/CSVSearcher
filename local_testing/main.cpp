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

	std::string tmp_filename = "tests/bm25_test_dir";
	bm25.save_to_disk(tmp_filename);

	_BM25 bm25_loaded(tmp_filename);
	printf("Loaded from disk\n"); fflush(stdout);
}

void test_multi_query() {
	const std::vector<std::vector<std::string>> MULTI_QUERIES = {
		{"DRAGON BALL Z", "GOKU"}
	};
	std::string FILENAME = "tests/wiki_articles.csv";
	std::vector<std::string> SEARCH_COLS = {"title", "body"};

	float BLOOM_DF_THRESHOLD = 0.005f;
	double BLOOM_FPR = 0.000001;
	float K1 = 1.2f;
	float B = 0.75f;
	uint16_t NUM_PARTITIONS = 24;

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

	for (const std::vector<std::string>& query : MULTI_QUERIES) {
		std::vector<std::string> _query = query;

		start = std::chrono::high_resolution_clock::now();
		std::vector<std::vector<std::pair<std::string, std::string>>> results = bm25.get_topk_internal_multi(
				_query,
				TOP_K,
				5000000,
				{2.0f, 1.0f}
				);

		for (const std::vector<std::pair<std::string, std::string>>& result : results) {
			printf("================================\n");
			for (const std::string& query : _query) {
				printf("Query: %s\n", query.c_str());
			}
			printf("--------------------------------\n");
			for (const std::pair<std::string, std::string>& res : result) {
				if (res.first == "title") {
					printf("Title: %s\n", res.second.c_str());
					continue;
				} else if (res.first == "body") {
					printf("Body: %s\n", res.second.c_str());
					continue;
				} else if (res.first == "score") {
					printf("Score: %s\n", res.second.c_str());
					continue;
				}
			}
			printf("--------------------------------\n");
			printf("================================\n");
		}
		end = std::chrono::high_resolution_clock::now();
		duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
		printf("Total Query time: %ld us\n\n", duration);
		printf("Number of results: %ld\n", results.size());
		printf("--------------------------------\n");
	}
}

void test_single_query() {
	const std::vector<std::string> QUERIES = {
		"UNDER MY SKIN",
		"THE BEATLES",
		"MY WAY",
		"TERMINATOR",
		"THE GODFATHER",
		"THE GODFATHER PART II",
		"SONG FOR GUY"
	};

	std::string FILENAME = "tests/mb_small.csv";
	std::vector<std::string> SEARCH_COLS = {"title", "artist"};

	float BLOOM_DF_THRESHOLD = 0.005f;
	double BLOOM_FPR = 0.000001;
	float K1 = 1.2f;
	float B = 0.75f;
	uint16_t NUM_PARTITIONS = 24;

	uint16_t TOP_K = 10;

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

	for (const std::string& query : QUERIES) {
		std::string _query = {query};
		start = std::chrono::high_resolution_clock::now();
		std::vector<std::vector<std::pair<std::string, std::string>>> results = bm25.get_topk_internal(
				_query,
				TOP_K,
				5000000,
				{2.0f, 1.0f}
				);
		end = std::chrono::high_resolution_clock::now();
		duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
		printf("Total Query time: %ld us\n\n", duration);
		printf("Number of results: %ld\n", results.size());
		printf("--------------------------------\n");
	}
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
	// test_multi_query();
	// bloom_test();
	test_save_load();
	return 0;
}
