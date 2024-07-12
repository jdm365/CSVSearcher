#include <stdio.h>

#include <chrono>
#include <vector>
#include <string>

#include "../bm25/engine.h"

#ifdef NDEBUG
	#undef NDEBUG
#endif


const std::vector<std::string> QUERIES = {
	"UNDER MY SKIN",
	"THE BEATLES",
	"MY WAY",
	"TERMINATOR",
	"THE GODFATHER",
	"THE GODFATHER PART II",
	"SONG FOR GUY"
};


int main() {
	// std::string FILENAME = "tests/mb_small.csv";
	// std::vector<std::string> SEARCH_COLS = {"title", "artist"};

	std::string FILENAME = "tests/wiki_articles.csv";
	std::vector<std::string> SEARCH_COLS = {"title", "body"};

	float BLOOM_DF_THRESHOLD = 0.005f;
	double BLOOM_FPR = 0.000001;
	float K1 = 1.2f;
	float B = 0.75f;
	uint16_t NUM_PARTITIONS = 24;

	uint16_t TOP_K = 1000;

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

		/*
		printf("Query: %s\n", query.c_str());
		printf("--------------------------------\n");
		for (const auto& result : results) {
			for (const auto& pair : result) {
				if (pair.first == "score") {
					printf("Score: %s\n", pair.second.c_str());
					continue;
				} else if (pair.first == "title") {
					printf("Title: %s\n", pair.second.c_str());
					continue;
				} else if (pair.first == "artist") {
					printf("Artist: %s\n", pair.second.c_str());
					continue;
				}
			}
			printf("--------------------------------\n");
			printf("\n");
		}
		printf("\n");
		*/
	}

	return 0;
}
