#include <iostream>
#include <string>
#include <unistd.h>
#include <chrono>

#include <stdint.h>


#include "../engine.h"

int main() {
	std::string query = "apple";

	// std::string FILENAME   = "/home/jdm365/SearchApp/data/companies_sorted_100M.csv";
	std::string FILENAME   = "/home/jdm365/search-benchmark-game/corpus.json";
	// std::string FILENAME   = "/home/jdm365/SearchApp/data/companies_sorted.csv";
	// std::string FILENAME   = "/home/jdm365/SearchApp/data/companies_sorted_name_only.csv";
	// std::string FILENAME   = "/home/jdm365/SearchApp/data/companies_sorted_1M.csv";
	// std::string FILENAME   = "/home/jdm365/SearchApp/data/companies_sorted_100k.csv";
	std::string SEARCH_COL = "text";

	// Get size of FILENAME in bytes
	FILE* file = fopen(FILENAME.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Error opening file" << std::endl;
		return 1;
	}
	fseek(file, 0, SEEK_END);
	uint64_t size_mb = ftell(file) / (1024 * 1024);
	fclose(file);

	const int   min_df = 1;
	const float max_df = 1.0f;
	const float k1 	   = 1.25f;
	const float b 	   = 0.75f;

	auto init = std::chrono::high_resolution_clock::now();
	_BM25 bm25(
			FILENAME,
			SEARCH_COL,
			min_df,
			max_df,
			k1,
			b
	);
	auto end = std::chrono::high_resolution_clock::now();
	auto time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - init).count();
	std::cout << "Time taken: " << time_ms << "ms" << std::endl;
	std::cout << "Mb/s: " << size_mb / (time_ms / 1000.0) << std::endl;
	std::cout << "Size: " << size_mb << "MB" << std::endl;

	for (auto& result : bm25.get_topk_internal(query, 10, 10)) {
		for (auto& r : result) {
			std::cout << r.first << " " << r.second << std::endl;
		}
	}

	return 0;
}
