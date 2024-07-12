#include <stdio.h>

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
	std::string FILENAME = "tests/mb.csv";
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

	for (const std::string& query : QUERIES) {
		std::string _query = {query};
		std::vector<std::vector<std::pair<std::string, std::string>>> results = bm25.get_topk_internal(
				_query,
				10,
				50000,
				{2.0f, 1.0f}
				);

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
	}

	return 0;
}
