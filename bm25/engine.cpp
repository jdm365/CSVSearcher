#include <unistd.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>
#include <stdio.h>

#include <iostream>
#include <vector>
#include <queue>
#include <string>
#include <cstdint>
#include <cmath>
#include <fstream>
#include <sstream>
#include <algorithm>

#include <chrono>
#include <ctime>
#include <sys/mman.h>
#include <fcntl.h>

#include "engine.h"
#include "robin_hood.h"
#include "xxhash64.h"


std::vector<std::string> process_csv(const char* filepath) {
    int fd = open(filepath, O_RDONLY);
    if (fd == -1) {
        std::cerr << "Error opening file: " << std::strerror(errno) << std::endl;
        return {};
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        std::cerr << "Error getting file size: " << std::strerror(errno) << std::endl;
        close(fd);
        return {};
    }

	char* mapped = (char*)mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        std::cerr << "Error mapping file: " << std::strerror(errno) << std::endl;
        close(fd);
        return {};
    }
	std::cout << "SB size: " << sb.st_size << std::endl;

    // Allocate a char buffer to hold the file's contents
    // Ensure there's enough memory for this operation to prevent bad_alloc exceptions
    // char* buffer = new char[sb.st_size];

    // Copy the contents from the memory-mapped area to the buffer
    // std::memcpy(buffer, mapped, sb.st_size);
	constexpr uint64_t COMMA_MASK   = 0x2C2C2C2C2C2C2C2C;
	constexpr uint64_t NEWLINE_MASK = 0x0A0A0A0A0A0A0A0A;
	constexpr uint64_t SPACE_MASK   = 0x2020202020202020;

	// Read the file line up until comma
	std::vector<std::string> names;
	names.reserve(sb.st_size / 8);
	std::string name = "";
	/*
	for (size_t i = 0; i < sb.st_size; ++i) {
		switch (mapped[i]) {
			case ' ':
				names.push_back(name);
				name.clear();
				break;
			case ',':
				names.push_back(name);
				name.clear();
				break;
			case '\n':
				names.push_back(name);
				break;
			default:
				name += mapped[i];
				break;
		}
	}
	*/
	std::vector<uint16_t> lengths;
	std::vector<uint64_t> starts;

	for (size_t i = 0; i < sb.st_size; i += 8) {
		uint64_t chunk = *(uint64_t*)(mapped + i);
		uint64_t delim_mask = chunk & (COMMA_MASK | NEWLINE_MASK | SPACE_MASK);

		int num_delims = __builtin_popcountll(delim_mask);
		for (int j = 0; j < num_delims; ++j) {
			uint64_t delim_idx = __builtin_ctzll(delim_mask);
			lengths.push_back(delim_idx);
			starts.push_back(i + delim_idx);
		}
	}

    // Cleanup
	// delete [] buffer;
    munmap(mapped, sb.st_size);
    close(fd);

	return names;
}


void _BM25::read_json(std::vector<uint64_t>& terms) {
	// Open the file
	FILE* file = fopen(filename.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << filename << std::endl;
		exit(1);
	}

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;

	uint64_t unique_terms_found = 0;

	while ((read = getline(&line, &len, file)) != -1) {
		line_offsets.push_back(ftello(file) - read);
		++line_num;

		// Iterate of line chars until we get to relevant column.
		int char_idx   = 0;
		while (true) {
			start:
			if (line[char_idx] == '"') {
				// Found first key. Match against search_col
				++char_idx;

				for (const char& c : search_col) {
					if (c != line[char_idx]) {
						// Scan to next key and then goto start.
						// Basically count until four unescaped quotes
						// are found the goto start
						size_t num_quotes = 0;
						while (num_quotes < 4) {
							num_quotes += (line[char_idx] == '"');
							++char_idx;
						}
						--char_idx;
						goto start;
					}
					++char_idx;
				}

				if (line[char_idx] == '"') {
					// If we made it here we found the correct key.
					// Now iterate just past 1 more unescaped num_quotes
					// to get to the value.
					++char_idx;
					while (line[char_idx] != '"') {
						++char_idx;
					}
					++char_idx;
					break;
				}
			}
			else if (line[char_idx] == '}') {
				std::cout << "Search field not found on line: " << line_num << std::endl;
				std::exit(1);
			}
			++char_idx;

			if (char_idx > 100000) {
				std::cout << "Error in read json" << std::endl;
				std::exit(1);
			}
		}
		if (line_num % 100000 == 0) {
			std::cout << "Lines read: " << line_num << std::endl;
		}

		// Split by commas not inside double quotes
		std::string doc = "";
		uint32_t doc_size = 0;
		while (line[char_idx] != '"') {
			if (line[char_idx] == ' ' && doc == "") {
				++char_idx;
				continue;
			}

			if (line[char_idx] == ' ') {
				auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);
				unique_terms_found += (uint64_t)add;
				terms.push_back(it->second);

				++doc_size;
				doc.clear();
			}
			else {
				doc += toupper(line[char_idx]);
			}
			++char_idx;

			if (char_idx > 10000000) {
				std::cout << "String too large. Code is broken" << std::endl;
				std::exit(1);
			}
		}
		auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);
		unique_terms_found += (uint64_t)add;
		terms.push_back(it->second);
		++doc_size;
		doc_sizes.push_back(doc_size);
	}
	fclose(file);

	num_docs = doc_sizes.size();
	std::cout << "Million Terms: " << terms.size() / 1000000 << std::endl;
	std::cout << "Num docs: " << num_docs << std::endl;
}

void _BM25::read_csv(std::vector<uint64_t>& terms) {
	// Open the file
	FILE* file = fopen(filename.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << filename << std::endl;
		exit(1);
	}

	int search_column_index = -1;

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;
	uint64_t byte_offset = 0;

	// Get col names
	read = getline(&line, &len, file);
	std::istringstream iss(line);
	std::string value;
	while (std::getline(iss, value, ',')) {
		if (value.find("\n") != std::string::npos) {
			value.erase(value.find("\n"));
		}
		columns.push_back(value);
		if (value == search_col) {
			search_column_index = columns.size() - 1;
		}
	}

	if (search_column_index == -1) {
		std::cerr << "Search column not found in header" << std::endl;
		for (int i = 0; i < columns.size(); ++i) {
			std::cerr << columns[i] << ",";
		}
		std::cerr << std::endl;
		exit(1);
	}
	byte_offset += ftell(file);

	uint64_t unique_terms_found = 0;

	std::string doc = "";

	// Small string optimization limit on most platforms
	doc.reserve(22);

	while ((read = getline(&line, &len, file)) != -1) {
		line_offsets.push_back(byte_offset);
		byte_offset += read;

		++line_num;

		// Iterate of line chars until we get to relevant column.
		int char_idx = 0;
		int col_idx  = 0;
		bool in_quotes = false;
		while (col_idx != search_column_index) {
			if (line[char_idx] == '"') {
				// Skip to next quote.
				++char_idx;
				while (line[char_idx] == '"') {
					++char_idx;
				}
			}

			if (line[char_idx] == ',') {
				++col_idx;
			}
			++char_idx;
		}

		// Split by commas not inside double quotes
		uint32_t doc_size = 0;
		char end_delim = ',';
		if (line[char_idx] == '"') {
			end_delim = '"';
			++char_idx;
		}

		while (line[char_idx] != end_delim) {
			if (line[char_idx] == ' ') {
				auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);
				unique_terms_found += (uint64_t)add;
				terms.push_back(it->second);
				++doc_size;
				doc.clear();

				++char_idx;
				continue;
			}
			doc += toupper(line[char_idx]);
			++char_idx;
		}
		auto [it, add] = unique_term_mapping.try_emplace(doc, unique_terms_found);
		unique_terms_found += (uint64_t)add;
		terms.push_back(it->second);
		++doc_size;
		doc_sizes.push_back(doc_size);
		doc.clear();
	}

	fclose(file);

	num_docs = doc_sizes.size();
}

void _BM25::read_csv_hash(std::vector<uint64_t>& terms) {
	// Open the file
	FILE* file = fopen(filename.c_str(), "r");
	if (file == NULL) {
		std::cerr << "Unable to open file: " << filename << std::endl;
		exit(1);
	}

	int search_column_index = -1;

	// Read the file line by line
	char*    line = NULL;
	size_t   len = 0;
	ssize_t  read;
	uint64_t line_num = 0;
	uint64_t byte_offset = 0;

	// Get col names
	read = getline(&line, &len, file);
	std::istringstream iss(line);
	std::string value;
	while (std::getline(iss, value, ',')) {
		if (value.find("\n") != std::string::npos) {
			value.erase(value.find("\n"));
		}
		columns.push_back(value);
		if (value == search_col) {
			search_column_index = columns.size() - 1;
		}
	}

	if (search_column_index == -1) {
		std::cerr << "Search column not found in header" << std::endl;
		for (int i = 0; i < columns.size(); ++i) {
			std::cerr << columns[i] << ",";
		}
		std::cerr << std::endl;
		exit(1);
	}
	byte_offset += ftell(file);

	std::string doc = "";

	// Small string optimization limit on most platforms
	doc.reserve(22);

	while ((read = getline(&line, &len, file)) != -1) {
		line_offsets.push_back(byte_offset);
		byte_offset += read;

		++line_num;

		// Iterate of line chars until we get to relevant column.
		int char_idx = 0;
		int col_idx  = 0;
		bool in_quotes = false;
		while (col_idx != search_column_index) {
			if (line[char_idx] == '"') {
				// Skip to next quote.
				++char_idx;
				while (line[char_idx] == '"') {
					++char_idx;
				}
			}

			if (line[char_idx] == ',') {
				++col_idx;
			}
			++char_idx;
		}

		// Split by commas not inside double quotes
		uint32_t doc_size = 0;
		char end_delim = ',';
		if (line[char_idx] == '"') {
			end_delim = '"';
			++char_idx;
		}

		while (line[char_idx] != end_delim) {
			if (line[char_idx] == ' ') {
				terms.push_back(hasher.hash(doc.data(), doc.size(), SEED));
				++doc_size;
				doc.clear();

				++char_idx;
				continue;
			}
			doc += toupper(line[char_idx]);
			++char_idx;
		}
		terms.push_back(hasher.hash(doc.data(), doc.size(), SEED));
		++doc_size;
		doc_sizes.push_back(doc_size);
		doc.clear();
	}

	fclose(file);

	num_docs = doc_sizes.size();
}


std::vector<std::pair<std::string, std::string>> _BM25::get_csv_line(int line_num) {
	std::ifstream file(filename);
	std::string line;
	file.seekg(line_offsets[line_num]);
	std::getline(file, line);
	file.close();

	// Create effective json by combining column names with values split by commas
	std::vector<std::pair<std::string, std::string>> row;
	std::string cell;
	bool in_quotes = false;
	size_t col_idx = 0;

	for (size_t i = 0; i < line.size(); ++i) {
		if (line[i] == '"') {
			in_quotes = !in_quotes;
		}
		else if (line[i] == ',' && !in_quotes) {
			row.emplace_back(columns[col_idx], cell);
			cell.clear();
			++col_idx;
		}
		else {
			cell += line[i];
		}
	}
	row.emplace_back(columns[col_idx], cell);
	return row;
}


std::vector<std::pair<std::string, std::string>> _BM25::get_json_line(int line_num) {
	std::ifstream file(filename);
	std::string line;
	file.seekg(line_offsets[line_num]);
	std::getline(file, line);
	file.close();

	// Create effective json by combining column names with values split by commas
	std::vector<std::pair<std::string, std::string>> row;
	bool in_quotes = false;
	size_t col_idx = 0;

	std::string first  = "";
	std::string second = "";

	while (true) {
		while (line[col_idx] != '"') {
			++col_idx;
		}
		++col_idx;

		while (line[col_idx] != '"') {
			first += line[col_idx];
			++col_idx;
		}
		++col_idx;

		while (line[col_idx] != '"') {
			++col_idx;
		}
		++col_idx;

		while (line[col_idx] != '"') {
			second += line[col_idx];
			++col_idx;
		}

		row.emplace_back(first, second);
		first.clear();
		second.clear();

		++col_idx;
		
		if (line[col_idx] == '}') {
			return row;
		}
	}
	return row;
}

void serialize_vector_u32(const std::vector<uint32_t>& vec, const std::string& filename) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file when serializing u32 vector.\n";
        return;
    }

    // Write the size of the vector first
    size_t size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

    // Write the vector elements
	for (const auto& val : vec) {
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(uint32_t));
	}

    out_file.close();
}

void serialize_vector_u64(const std::vector<uint64_t>& vec, const std::string& filename) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file when serializing u64 vector.\n";
        return;
    }

    // Write the size of the vector first
    size_t size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

    // Write the vector elements
	for (const auto& val : vec) {
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(uint64_t));
	}

    out_file.close();
}

void serialize_vector_of_vectors_u32(
		const std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector first
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    for (const auto& inner_vec : vec) {
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the elements of the inner vector
        if (!inner_vec.empty()) {
            out_file.write(reinterpret_cast<const char*>(inner_vec.data()), inner_vec.size() * sizeof(uint32_t));
        }
    }

    out_file.close();
}

void serialize_vector_of_vectors_u64(
		const std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector first
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    for (const auto& inner_vec : vec) {
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the elements of the inner vector
        if (!inner_vec.empty()) {
            out_file.write(reinterpret_cast<const char*>(inner_vec.data()), inner_vec.size() * sizeof(uint64_t));
        }
    }

    out_file.close();
}

void serialize_robin_hood_flat_map_string_u32(
		const robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the map first
    size_t map_size = map.size();
    out_file.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

    for (const auto& [key, value] : map) {
        size_t key_size = key.size();
        out_file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out_file.write(key.data(), key_size);
        out_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    out_file.close();
}

void serialize_robin_hood_flat_map_string_u64(
		const robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the map first
    size_t map_size = map.size();
    out_file.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

    for (const auto& [key, value] : map) {
        size_t key_size = key.size();
        out_file.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out_file.write(key.data(), key_size);
        out_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    out_file.close();
}

void serialize_vector_of_vectors_pair_u32_u16(
		const std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    // Iterate through the outer vector
    for (const auto& inner_vec : vec) {
        // Write the size of the inner vector
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the pairs
        for (const auto& [first, second] : inner_vec) {
            out_file.write(reinterpret_cast<const char*>(&first), sizeof(first));
            out_file.write(reinterpret_cast<const char*>(&second), sizeof(second));
        }
    }

    out_file.close();
}

void serialize_vector_of_vectors_pair_u64_u16(
		const std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the outer vector
    size_t outer_size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    // Iterate through the outer vector
    for (const auto& inner_vec : vec) {
        // Write the size of the inner vector
        size_t inner_size = inner_vec.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size), sizeof(inner_size));
        
        // Write the pairs
        for (const auto& [first, second] : inner_vec) {
            out_file.write(reinterpret_cast<const char*>(&first), sizeof(first));
            out_file.write(reinterpret_cast<const char*>(&second), sizeof(second));
        }
    }

    out_file.close();
}


void deserialize_vector_u32(std::vector<uint32_t>& vec, const std::string& filename) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the vector
    size_t size;
    in_file.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Resize the vector and read its elements
    vec.resize(size);
    if (size > 0) {
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(uint32_t));
    }

    in_file.close();
}

void deserialize_vector_u64(std::vector<uint64_t>& vec, const std::string& filename) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the vector
    size_t size;
    in_file.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Resize the vector and read its elements
    vec.resize(size);
    if (size > 0) {
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(uint64_t));
    }

    in_file.close();
}

void deserialize_vector_of_vectors_u32(
		std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	if (vec.size() > 0) {
		vec.clear();
	}

	// Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    // Iterate through the outer vector
    for (auto& inner_vec : vec) {
        // Read the size of the inner vector
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the elements of the inner vector
        if (inner_size > 0) {
            in_file.read(reinterpret_cast<char*>(inner_vec.data()), inner_size * sizeof(uint32_t));
        }
    }

    in_file.close();
}


void deserialize_vector_of_vectors_u64(
		std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	if (vec.size() > 0) {
		vec.clear();
	}

	// Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    // Iterate through the outer vector
    for (auto& inner_vec : vec) {
        // Read the size of the inner vector
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the elements of the inner vector
        if (inner_size > 0) {
            in_file.read(reinterpret_cast<char*>(inner_vec.data()), inner_size * sizeof(uint64_t));
        }
    }

    in_file.close();
}

void deserialize_robin_hood_flat_map_string_u32(
		robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	// Read the size of the map
    size_t map_size;
    in_file.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

    // Read each key-value pair and insert it into the map
    for (size_t i = 0; i < map_size; ++i) {
        size_t keySize;
        in_file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
        
        std::string key(keySize, '\0');
        in_file.read(&key[0], keySize);

        uint32_t value;
        in_file.read(reinterpret_cast<char*>(&value), sizeof(value));

        map[key] = value;
    }

    in_file.close();
}

void deserialize_robin_hood_flat_map_string_u64(
		robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

	// Read the size of the map
    size_t map_size;
    in_file.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

    // Read each key-value pair and insert it into the map
    for (size_t i = 0; i < map_size; ++i) {
        size_t keySize;
        in_file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
        
        std::string key(keySize, '\0');
        in_file.read(&key[0], keySize);

        uint64_t value;
        in_file.read(reinterpret_cast<char*>(&value), sizeof(value));

        map[key] = value;
    }

    in_file.close();
}

void deserialize_vector_of_vectors_pair_u32_u16(
		std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    for (auto& inner_vec : vec) {
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the pairs
        for (auto& [first, second] : inner_vec) {
            in_file.read(reinterpret_cast<char*>(&first), sizeof(first));
            in_file.read(reinterpret_cast<char*>(&second), sizeof(second));
        }
    }

    in_file.close();
}

void deserialize_vector_of_vectors_pair_u64_u16(
		std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		) {
	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read the size of the outer vector
    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    vec.resize(outer_size);

    for (auto& inner_vec : vec) {
        size_t inner_size;
        in_file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
        inner_vec.resize(inner_size);

        // Read the pairs
        for (auto& [first, second] : inner_vec) {
            in_file.read(reinterpret_cast<char*>(&first), sizeof(first));
            in_file.read(reinterpret_cast<char*>(&second), sizeof(second));
        }
    }

    in_file.close();
}

void _BM25::save_to_disk() {
	auto start = std::chrono::high_resolution_clock::now();

	if (access(DIR_NAME.c_str(), F_OK) != -1) {
		// Remove the directory if it exists
		std::string command = "rm -r " + DIR_NAME;
		system(command.c_str());

		// Create the directory
		command = "mkdir " + DIR_NAME;
		system(command.c_str());
	}
	else {
		// Create the directory if it does not exist
		std::string command = "mkdir " + DIR_NAME;
		system(command.c_str());
	}

	serialize_robin_hood_flat_map_string_u64(unique_term_mapping, UNIQUE_TERM_MAPPING_PATH);
	serialize_vector_of_vectors_u64(inverted_index, INVERTED_INDEX_PATH);
	serialize_vector_of_vectors_pair_u64_u16(term_freqs, TERM_FREQS_FILE_PATH);
	serialize_vector_u64(doc_term_freqs, DOC_TERM_FREQS_PATH);
	serialize_vector_u64(doc_sizes, DOC_SIZES_PATH);
	serialize_vector_u64(line_offsets, LINE_OFFSETS_PATH);

	// Serialize smaller members.
	std::ofstream out_file(METADATA_PATH, std::ios::binary);
	if (!out_file) {
		std::cerr << "Error opening file for writing.\n";
		return;
	}

	// Write basic types directly
	out_file.write(reinterpret_cast<const char*>(&num_docs), sizeof(num_docs));
	out_file.write(reinterpret_cast<const char*>(&min_df), sizeof(min_df));
	out_file.write(reinterpret_cast<const char*>(&avg_doc_size), sizeof(avg_doc_size));
	out_file.write(reinterpret_cast<const char*>(&max_df), sizeof(max_df));
	out_file.write(reinterpret_cast<const char*>(&k1), sizeof(k1));
	out_file.write(reinterpret_cast<const char*>(&b), sizeof(b));

	// Write enum as int
	int file_type_int = static_cast<int>(file_type);
	out_file.write(reinterpret_cast<const char*>(&file_type_int), sizeof(file_type_int));

	// Write std::string
	size_t filename_length = filename.size();
	out_file.write(reinterpret_cast<const char*>(&filename_length), sizeof(filename_length));
	out_file.write(filename.data(), filename_length);

	// Write std::vector<std::string>
	size_t columns_size = columns.size();
	out_file.write(reinterpret_cast<const char*>(&columns_size), sizeof(columns_size));
	for (const auto& col : columns) {
		size_t col_length = col.size();
		out_file.write(reinterpret_cast<const char*>(&col_length), sizeof(col_length));
		out_file.write(col.data(), col_length);
	}

	// Write search_col std::string
	size_t search_col_length = search_col.size();
	out_file.write(reinterpret_cast<const char*>(&search_col_length), sizeof(search_col_length));
	out_file.write(search_col.data(), search_col_length);

	out_file.close();

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Saved in " << elapsed_seconds.count() << "s" << std::endl;
	}
}

void _BM25::load_from_disk(const std::string& db_dir) {
	auto start = std::chrono::high_resolution_clock::now();

	deserialize_robin_hood_flat_map_string_u64(unique_term_mapping, UNIQUE_TERM_MAPPING_PATH);
	deserialize_vector_of_vectors_u64(inverted_index, INVERTED_INDEX_PATH);
	deserialize_vector_of_vectors_pair_u64_u16(term_freqs, TERM_FREQS_FILE_PATH);
	deserialize_vector_u64(doc_term_freqs, DOC_TERM_FREQS_PATH);
	deserialize_vector_u64(doc_sizes, DOC_SIZES_PATH);
	deserialize_vector_u64(line_offsets, LINE_OFFSETS_PATH);

	// Load smaller members.
	std::ifstream in_file(METADATA_PATH, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    // Read basic types directly
    in_file.read(reinterpret_cast<char*>(&num_docs), sizeof(num_docs));
    in_file.read(reinterpret_cast<char*>(&min_df), sizeof(min_df));
    in_file.read(reinterpret_cast<char*>(&avg_doc_size), sizeof(avg_doc_size));
    in_file.read(reinterpret_cast<char*>(&max_df), sizeof(max_df));
    in_file.read(reinterpret_cast<char*>(&k1), sizeof(k1));
    in_file.read(reinterpret_cast<char*>(&b), sizeof(b));

    // Read enum as int
    int file_type_int;
    in_file.read(reinterpret_cast<char*>(&file_type_int), sizeof(file_type_int));
    file_type = static_cast<SupportedFileTypes>(file_type_int);

    // Read std::string
    size_t filename_length;
    in_file.read(reinterpret_cast<char*>(&filename_length), sizeof(filename_length));
    filename.resize(filename_length);
    in_file.read(&filename[0], filename_length);

    // Read std::vector<std::string>
    size_t columns_size;
    in_file.read(reinterpret_cast<char*>(&columns_size), sizeof(columns_size));
    columns.resize(columns_size);
    for (auto& col : columns) {
        size_t col_length;
        in_file.read(reinterpret_cast<char*>(&col_length), sizeof(col_length));
        col.resize(col_length);
        in_file.read(&col[0], col_length);
    }

    // Read search_col std::string
    size_t search_col_length;
    in_file.read(reinterpret_cast<char*>(&search_col_length), sizeof(search_col_length));
    search_col.resize(search_col_length);
    in_file.read(&search_col[0], search_col_length);

    in_file.close();

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Loaded in " << elapsed_seconds.count() << "s" << std::endl;
	}
}

uint64_t _BM25::get_doc_term_freq_db(const uint64_t& term_idx) {
	return doc_term_freqs[term_idx];
	// return doc_term_freqs_map[term_idx];
}

std::vector<uint64_t> _BM25::get_inverted_index_db(const uint64_t& term_idx) {
	return inverted_index[term_idx];
	// return inverted_index_map[term_idx];
}


uint16_t _BM25::get_term_freq_from_file(
		int line_num,
		const uint64_t& term_idx
		) {
	// Default to 1.0f.
	// Inverted index implies membership.
	// Only store values with >1 term freq.
	// float tf = 1.0f;
	float tf = 0.0f;
	for (const std::pair<uint64_t, uint16_t>& term_freq : term_freqs[line_num]) {
		if (term_freq.first == term_idx) {
			tf = term_freq.second;
			return tf;
		}
	}
	return tf;
}


_BM25::_BM25(
		std::string filename,
		std::string search_col,
		int min_df,
		float max_df,
		float k1,
		float b
		) : min_df(min_df), 
			max_df(max_df), 
			k1(k1), 
			b(b),
			search_col(search_col), 
			filename(filename) {

	auto overall_start = std::chrono::high_resolution_clock::now();
	
	// Read file to get documents, line offsets, and columns
	std::vector<uint64_t> terms;
	if (filename.substr(filename.size() - 3, 3) == "csv") {
		read_csv(terms);
		// read_csv_hash(terms);
		file_type = CSV;
	}
	else if (filename.substr(filename.size() - 4, 4) == "json") {
		read_json(terms);
		file_type = JSON;
	}
	else {
		std::cout << "Only csv and json files are supported." << std::endl;
		std::exit(1);
	}

	auto read_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> read_elapsed_seconds = read_end - overall_start;
	if (DEBUG) {
		std::cout << "Read file in " << read_elapsed_seconds.count() << " seconds" << std::endl;
	}

	auto start = std::chrono::high_resolution_clock::now();

	doc_term_freqs.resize(unique_term_mapping.size());
	inverted_index.resize(unique_term_mapping.size());
	term_freqs.resize(num_docs);

	// Accumulate document frequencies
	uint64_t terms_seen = 0;
	for (size_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		robin_hood::unordered_flat_set<uint64_t> seen_terms;

		uint64_t doc_size = doc_sizes[doc_id];
		uint64_t end = terms_seen + doc_size;

		for (uint64_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint64_t mapped_term_idx = terms[term_idx];

			if (seen_terms.find(mapped_term_idx) != seen_terms.end()) {
				++terms_seen;
				continue;
			}

			++doc_term_freqs[mapped_term_idx];
			/*
			if (doc_term_freqs_map.find(mapped_term_idx) == doc_term_freqs_map.end()) {
				doc_term_freqs_map[mapped_term_idx] = 1;
			}
			else {
				++doc_term_freqs_map[mapped_term_idx];
			}
			*/

			seen_terms.insert(mapped_term_idx);
			++terms_seen;
		}
	}
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished accumulating document frequencies in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();

	uint64_t max_number_of_occurrences = max_df * num_docs;
	robin_hood::unordered_set<uint64_t> blacklisted_terms;

	// Filter terms by min_df and max_df
	for (size_t term_id = 0; term_id < doc_term_freqs.size(); ++term_id) {
		uint64_t term_count = doc_term_freqs[term_id];
		if (term_count < min_df || term_count > max_number_of_occurrences) {
			blacklisted_terms.insert(term_id);
			doc_term_freqs[term_id] = 0;
		}
		inverted_index[term_id].reserve(term_count);
		// inverted_index_map[term_id].reserve(term_count);
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished filtering terms by min_df and max_df in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();
	terms_seen = 0;
	for (uint64_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		uint32_t doc_size = doc_sizes[doc_id];

		size_t end = terms_seen + doc_size;

		robin_hood::unordered_flat_map<uint64_t, uint16_t> local_term_freqs;
		local_term_freqs.reserve(doc_size);
		for (size_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint64_t mapped_term_idx = terms[term_idx];
			++terms_seen;

			if (blacklisted_terms.find(mapped_term_idx) != blacklisted_terms.end()) {
				continue;
			}

			if (local_term_freqs.find(mapped_term_idx) != local_term_freqs.end()) {
				++local_term_freqs[mapped_term_idx];
			}
			else {
				local_term_freqs[mapped_term_idx] = 1;
				inverted_index[mapped_term_idx].push_back(doc_id);
				// inverted_index_map[mapped_term_idx].push_back(doc_id);
			}
		}

		// Copy to term freqs
		for (const auto& pair : local_term_freqs) {
			term_freqs[doc_id].emplace_back(pair.first, pair.second);
		}
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Got term frequencies and inverted index in " << elapsed_seconds.count() << "s" << std::endl;
	}

	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - overall_start;
	if (DEBUG) {
		std::cout << "Finished creating BM25 index in " << elapsed_seconds.count() << "s" << std::endl;
	}

	uint64_t max_doc_size = 0;
	avg_doc_size = 0.0f;
	for (int doc_id = 0; doc_id < num_docs; ++doc_id) {
		avg_doc_size += (float)doc_sizes[doc_id];
		max_doc_size = std::max((uint64_t)doc_sizes[doc_id], max_doc_size);
	}
	avg_doc_size /= num_docs;

	if (DEBUG) {
		std::cout << "Largest doc size: " << max_doc_size << std::endl;
	}

	if (DEBUG) {
		uint64_t bytes_used = 0;
		for (const auto& vec : inverted_index) {
			bytes_used += sizeof(vec[0]) * vec.size();
		}
		std::cout << "Inverted Index MB: " << bytes_used / (1024 * 1024) << std::endl;

		bytes_used = 0;
		for (const auto& vec : term_freqs) {
			bytes_used += (sizeof(vec[0].first) + sizeof(vec[0].second)) * vec.size();
		}
		std::cout << "Term Freqs MB:     " << bytes_used / (1024 * 1024) << std::endl;
		std::cout << "Doc Term Freqs MB: " << (sizeof(doc_term_freqs[0]) * doc_term_freqs.size()) / (1024 * 1024) << std::endl;
		std::cout << "Doc Sizes MB:      " << (sizeof(doc_sizes[0]) * doc_sizes.size()) / (1024 * 1024) << std::endl;
		std::cout << "Line Offsets MB:   " << (sizeof(line_offsets[0]) * line_offsets.size()) / (1024 * 1024) << std::endl;

		std::cout << "Num unique terms:  " << unique_term_mapping.size() << std::endl;
	}
}


_BM25::_BM25(
		std::vector<std::string>& documents,
		int min_df,
		float max_df,
		float k1,
		float b
		) : min_df(min_df), 
			max_df(max_df), 
			k1(k1), 
			b(b) {
	filename = "in_memory";
	file_type = IN_MEMORY;

	auto overall_start = std::chrono::high_resolution_clock::now();

	num_docs = documents.size();

	std::vector<uint64_t> terms;
	terms.reserve(documents.size() * documents[0].size());

	uint64_t unique_terms_found = 0;
	std::string term = "";
	for (const std::string& doc : documents) {
		size_t char_idx = 0;
		size_t doc_size = 0;

		for (const char& c : doc) {
			if (c == ' ') {
				auto [it, add] = unique_term_mapping.try_emplace(term, unique_terms_found);
				unique_terms_found += (uint64_t)add;
				terms.push_back(it->second);
				term.clear();
				++doc_size;
				++char_idx;
				continue;
			}

			term += toupper(c);
			++char_idx;
		}
		if (term.size() > 0) {
			auto [it, add] = unique_term_mapping.try_emplace(term, unique_terms_found);
			unique_terms_found += (uint64_t)add;
			terms.push_back(it->second);
			term.clear();
			++doc_size;
		}
		doc_sizes.push_back(doc_size);
	}
	terms.shrink_to_fit();
	
	auto start = std::chrono::high_resolution_clock::now();

	doc_term_freqs.resize(unique_term_mapping.size());
	inverted_index.resize(unique_term_mapping.size());
	term_freqs.resize(num_docs);

	std::cout << "Unique terms found: " << unique_terms_found << std::endl;
	std::cout << std::flush;

	// Accumulate document frequencies
	uint64_t terms_seen = 0;
	for (size_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		robin_hood::unordered_flat_set<uint64_t> seen_terms;

		uint32_t doc_size = doc_sizes[doc_id];
		size_t end = terms_seen + doc_size;

		for (size_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint64_t mapped_term_idx = terms[term_idx];

			if (seen_terms.find(mapped_term_idx) != seen_terms.end()) {
				++terms_seen;
				continue;
			}

			++doc_term_freqs[mapped_term_idx];

			seen_terms.insert(mapped_term_idx);
			++terms_seen;
		}
	}
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished accumulating document frequencies in " << elapsed_seconds.count() << "s" << std::endl;
	}


	start = std::chrono::high_resolution_clock::now();

	uint64_t max_number_of_occurrences = max_df * num_docs;
	robin_hood::unordered_set<uint64_t> blacklisted_terms;

	// Filter terms by min_df and max_df
	for (size_t term_id = 0; term_id < doc_term_freqs.size(); ++term_id) {
		uint64_t term_count = doc_term_freqs[term_id];
		if (term_count < min_df || term_count > max_number_of_occurrences) {
			blacklisted_terms.insert(term_id);
			doc_term_freqs[term_id] = 0;
		}
		inverted_index[term_id].reserve(term_count);
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Finished filtering terms by min_df and max_df in " << elapsed_seconds.count() << "s" << std::endl;
	}

	start = std::chrono::high_resolution_clock::now();
	terms_seen = 0;
	for (uint64_t doc_id = 0; doc_id < num_docs; ++doc_id) {
		uint64_t doc_size = doc_sizes[doc_id];

		size_t end = terms_seen + doc_size;

		robin_hood::unordered_flat_map<uint64_t, uint16_t> local_term_freqs;
		local_term_freqs.reserve(doc_size);
		for (size_t term_idx = terms_seen; term_idx < end; ++term_idx) {
			uint64_t mapped_term_idx = terms[term_idx];
			++terms_seen;

			if (blacklisted_terms.find(mapped_term_idx) != blacklisted_terms.end()) {
				continue;
			}

			if (local_term_freqs.find(mapped_term_idx) != local_term_freqs.end()) {
				++local_term_freqs[mapped_term_idx];
			}
			else {
				local_term_freqs[mapped_term_idx] = 1;
				inverted_index[mapped_term_idx].push_back(doc_id);
			}
		}

		// Copy to term freqs
		for (const auto& pair : local_term_freqs) {
			term_freqs[doc_id].emplace_back(pair.first, pair.second);
		}
	}
	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - start;

	if (DEBUG) {
		std::cout << "Got term frequencies and inverted index in " << elapsed_seconds.count() << "s" << std::endl;
	}

	end = std::chrono::high_resolution_clock::now();
	elapsed_seconds = end - overall_start;
	if (DEBUG) {
		std::cout << "Finished creating BM25 index in " << elapsed_seconds.count() << "s" << std::endl;
	}

	uint64_t max_doc_size = 0;
	avg_doc_size = 0.0f;
	for (int doc_id = 0; doc_id < num_docs; ++doc_id) {
		avg_doc_size += (float)doc_sizes[doc_id];
		max_doc_size = std::max((uint64_t)doc_sizes[doc_id], max_doc_size);
	}
	avg_doc_size /= num_docs;
	std::cout << "Largest doc size: " << max_doc_size << std::endl;

	if (DEBUG) {
		uint64_t bytes_used = 0;
		for (const auto& vec : inverted_index) {
			bytes_used += sizeof(vec[0]) * vec.size();
		}
		std::cout << "Inverted Index MB: " << bytes_used / (1024 * 1024) << std::endl;

		bytes_used = 0;
		for (const auto& vec : term_freqs) {
			bytes_used += (sizeof(vec[0].first) + sizeof(vec[0].second)) * vec.size();
		}
		std::cout << "Term Freqs MB:     " << bytes_used / (1024 * 1024) << std::endl;
		std::cout << "Doc Term Freqs MB: " << (sizeof(doc_term_freqs[0]) * doc_term_freqs.size()) / (1024 * 1024) << std::endl;
		std::cout << "Doc Sizes MB:      " << (sizeof(doc_sizes[0]) * doc_sizes.size()) / (1024 * 1024) << std::endl;
		std::cout << "Line Offsets MB:   " << (sizeof(line_offsets[0]) * line_offsets.size()) / (1024 * 1024) << std::endl;

		std::cout << "Num unique terms:  " << unique_term_mapping.size() << std::endl;
	}
}

inline float _BM25::_compute_idf(const uint64_t& term_idx) {
	uint64_t df = get_doc_term_freq_db(term_idx);
	return log((num_docs - df + 0.5) / (df + 0.5));
}

inline float _BM25::_compute_bm25(
		uint64_t doc_id,
		float tf,
		float idf
		) {
	float doc_size = doc_sizes[doc_id];

	return idf * tf / (tf + k1 * (1 - b + b * doc_size / avg_doc_size));
}


std::vector<std::pair<uint64_t, float>> _BM25::query(
		std::string& query, 
		uint32_t k,
		uint32_t init_max_df 
		) {
	std::vector<uint64_t> term_idxs;

	std::string substr = "";
	for (const char& c : query) {
		if (c != ' ') {
			substr += c; 
			continue;
		}
		if (unique_term_mapping.find(substr) == unique_term_mapping.end()) {
			continue;
		}
		term_idxs.push_back(unique_term_mapping[substr]);
		substr.clear();
	}
	if (unique_term_mapping.find(substr) != unique_term_mapping.end()) {
		term_idxs.push_back(unique_term_mapping[substr]);
	}

	if (term_idxs.size() == 0) {
		return std::vector<std::pair<uint64_t, float>>();
	}

	// Gather docs that contain at least one term from the query
	// Uses dynamic max_df for performance
	int local_max_df = init_max_df;
	robin_hood::unordered_set<uint64_t> candidate_docs;

	while (candidate_docs.size() == 0) {
		for (const uint64_t& term_idx : term_idxs) {
			std::vector<uint64_t> doc_ids = get_inverted_index_db(term_idx);

			if (doc_ids.size() == 0) {
				continue;
			}

			if (doc_ids.size() > local_max_df) {
				continue;
			}

			for (const uint64_t& doc_id : doc_ids) {
				candidate_docs.insert(doc_id);
			}
		}
		local_max_df *= 20;

		if (local_max_df > num_docs || local_max_df > (int)max_df * num_docs) {
			break;
		}
	}
	
	if (candidate_docs.size() == 0) {
		return std::vector<std::pair<uint64_t, float>>();
	}

	// Priority queue to store top k docs
	// Largest to smallest scores
	std::priority_queue<
		std::pair<uint64_t, float>, 
		std::vector<std::pair<uint64_t, float>>, 
		_compare> top_k_docs;

	// Compute BM25 scores for each candidate doc
	std::vector<float> idfs(term_idxs.size(), 0.0f);
	int idx = 0;
	for (const uint64_t& doc_id : candidate_docs) {
		float score = 0;
		int   jdx = 0;
		for (const uint64_t& term_idx : term_idxs) {
			if (idx == 0) {
				idfs[jdx] = _compute_idf(term_idx);
			}
			float tf  = get_term_freq_from_file(doc_id, term_idx);
			score += _compute_bm25(doc_id, tf, idfs[jdx]);
			++jdx;
		}

		top_k_docs.push(std::make_pair(doc_id, score));
		if (top_k_docs.size() > k) {
			top_k_docs.pop();
		}
		++idx;
	}
	

	std::vector<std::pair<uint64_t, float>> result(top_k_docs.size());
	idx = top_k_docs.size() - 1;
	while (!top_k_docs.empty()) {
		result[idx] = top_k_docs.top();
		top_k_docs.pop();
		--idx;
	}

	return result;
}


std::vector<std::vector<std::pair<std::string, std::string>>> _BM25::get_topk_internal(
		std::string& _query,
		uint32_t top_k,
		uint32_t init_max_df
		) {
	std::vector<std::vector<std::pair<std::string, std::string>>> result;
	std::vector<std::pair<uint64_t, float>> top_k_docs = query(_query, top_k, init_max_df);
	result.reserve(top_k_docs.size());

	std::vector<std::pair<std::string, std::string>> row;
	for (size_t i = 0; i < top_k_docs.size(); ++i) {
		switch (file_type) {
			case CSV:
				row = get_csv_line(top_k_docs[i].first);
				break;
			case JSON:
				row = get_json_line(top_k_docs[i].first);
				break;
			case IN_MEMORY:
				std::cout << "Error: In-memory data not supported for this function." << std::endl;
				std::exit(1);
				break;
			default:
				std::cout << "Error: Incorrect file type" << std::endl;
				std::exit(1);
				break;
		}
		row.push_back(std::make_pair("score", std::to_string(top_k_docs[i].second)));
		result.push_back(row);
	}
	return result;
}
