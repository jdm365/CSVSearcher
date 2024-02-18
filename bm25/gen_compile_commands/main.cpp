#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unistd.h>


#include "../bm25_utils.h"
#include <leveldb/db.h>

int main() {
	std::string query = "apple";

	std::string FILENAME = "/home/jdm365/SearchApp/basic_search/data/companies_sorted.csv";

	std::vector<std::string> companies;
	std::ifstream file(FILENAME);
	std::string line;
	while (std::getline(file, line)) {
		std::string name = line.substr(0, line.find(","));
		companies.push_back(name);
	}

	std::cout << "Companies: " << companies.size() << std::endl;
	for (int idx = 0; idx < 10; ++idx) {
		std::cout << companies[idx] << std::endl;
	}

	_BM25 bm25(
			companies,
			true,
			3,
			1,
			0.5f,
			1.25f,
			0.75f
	);

	companies.clear();
	companies.shrink_to_fit();

	// Sleep for 20 seconds
	std::cout << "Sleeping for 20 seconds" << std::endl;
	sleep(20);
	return 0;
}
