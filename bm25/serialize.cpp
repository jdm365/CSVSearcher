#include <iostream>
#include <vector>
#include <string>
#include <fstream>


#include "serialize.h"
#include "robin_hood.h"
#include "bloom.h"



void serialize_vector_u8(const std::vector<uint8_t>& vec, const std::string& filename) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file when serializing u8 vector.\n";
        return;
    }

    // Write the size of the vector first
    size_t size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

    // Write the vector elements
	for (const auto& val : vec) {
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(uint8_t));
	}

    out_file.close();
}

void serialize_vector_u16(const std::vector<uint16_t>& vec, const std::string& filename) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file when serializing u8 vector.\n";
        return;
    }

    // Write the size of the vector first
    size_t size = vec.size();
    out_file.write(reinterpret_cast<const char*>(&size), sizeof(size));

    // Write the vector elements
	for (const auto& val : vec) {
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(uint16_t));
	}

    out_file.close();
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

void serialize_vector_float(const std::vector<float>& vec, const std::string& filename) {
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
    	out_file.write(reinterpret_cast<const char*>(&val), sizeof(float));
	}

    out_file.close();
}

void serialize_vector_of_vectors_u8(
		const std::vector<std::vector<uint8_t>>& vec, 
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
            out_file.write(
					reinterpret_cast<const char*>(inner_vec.data()), 
					inner_vec.size() * sizeof(uint8_t)
					);
        }
    }

    out_file.close();
}

void serialize_inverted_index(
    const InvertedIndex& II, 
    const std::string& filename
) {
    std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    size_t outer_size = II.inverted_index_compressed.size();
    out_file.write(reinterpret_cast<const char*>(&outer_size), sizeof(outer_size));

    for (uint64_t idx = 0; idx < outer_size; ++idx) {
        size_t inner_size_doc_ids = II.inverted_index_compressed[idx].doc_ids.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size_doc_ids), sizeof(inner_size_doc_ids));
        
        if (!II.inverted_index_compressed[idx].doc_ids.empty()) {
            out_file.write(
                reinterpret_cast<const char*>(II.inverted_index_compressed[idx].doc_ids.data()),
                II.inverted_index_compressed[idx].doc_ids.size() * sizeof(uint8_t)
            );
        }

        size_t inner_size_term_freqs = II.inverted_index_compressed[idx].term_freqs.size();
        out_file.write(reinterpret_cast<const char*>(&inner_size_term_freqs), sizeof(inner_size_term_freqs));

        if (!II.inverted_index_compressed[idx].term_freqs.empty()) {
            out_file.write(
                reinterpret_cast<const char*>(II.inverted_index_compressed[idx].term_freqs.data()),
                II.inverted_index_compressed[idx].term_freqs.size() * sizeof(RLEElement_u8)
            );
        }
    }

    out_file.close();

	// Serialize bloom filters.
	uint16_t idx = 0;
	for (const auto& bloom_pair : II.bloom_filters) {
		const char* new_filename = (filename + "_bloom_" + std::string(idx)).c_str();

		// TODO: Save doc_id.
		serialize_bloom_entry(bloom_pair.second, new_filename);
	}

}

void deserialize_inverted_index(
    InvertedIndex& II, 
    const std::string& filename
) {
    std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return;
    }

    size_t outer_size;
    in_file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    II.inverted_index_compressed.resize(outer_size);

    for (uint64_t idx = 0; idx < outer_size; ++idx) {
        size_t inner_size_doc_ids;
        in_file.read(reinterpret_cast<char*>(&inner_size_doc_ids), sizeof(inner_size_doc_ids));
        II.inverted_index_compressed[idx].doc_ids.resize(inner_size_doc_ids);

        if (inner_size_doc_ids > 0) {
            in_file.read(
                reinterpret_cast<char*>(II.inverted_index_compressed[idx].doc_ids.data()),
                inner_size_doc_ids * sizeof(uint8_t)
            );
        }

        size_t inner_size_term_freqs;
        in_file.read(reinterpret_cast<char*>(&inner_size_term_freqs), sizeof(inner_size_term_freqs));
        II.inverted_index_compressed[idx].term_freqs.resize(inner_size_term_freqs);

        if (inner_size_term_freqs > 0) {
            in_file.read(
                reinterpret_cast<char*>(II.inverted_index_compressed[idx].term_freqs.data()),
                inner_size_term_freqs * sizeof(RLEElement_u8)
            );
        }
    }

    in_file.close();
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

void deserialize_vector_u8(std::vector<uint8_t>& vec, const std::string& filename) {
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
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(uint8_t));
    }

    in_file.close();
}

void deserialize_vector_u16(std::vector<uint16_t>& vec, const std::string& filename) {
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
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(uint16_t));
    }

    in_file.close();
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

void deserialize_vector_float(std::vector<float>& vec, const std::string& filename) {
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
        in_file.read(reinterpret_cast<char*>(&vec[0]), size * sizeof(float));
    }

    in_file.close();
}

void deserialize_vector_of_vectors_u8(
		std::vector<std::vector<uint8_t>>& vec, 
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
            in_file.read(reinterpret_cast<char*>(inner_vec.data()), inner_size * sizeof(uint8_t));
        }
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


void serialize_bloom_entry(
		const BloomEntry& bloom_entry,
		const char* filename
		) {
	std::ofstream out_file(filename, std::ios::binary);
    if (!out_file) {
        std::cerr << "Error opening file for writing.\n";
        return;
    }

    // Write the size of the map first
    size_t map_size = bloom_entry.bloom_filters.size();
    out_file.write(
			reinterpret_cast<const char*>(&map_size), 
			sizeof(map_size)
			);

	for (const auto& [tf, filter] : bloom_entry.bloom_filters) {
        out_file.write(
				reinterpret_cast<const char*>(&tf), 
				sizeof(uint16_t)
				);
		uint64_t size = (filter.num_bits + 7) / 8;
    	out_file.write(
				reinterpret_cast<const char*>(&size), 
				sizeof(uint64_t)
				);
		bloom_save(filter, filename);
	}
    out_file.close();

	// Save topk doc_ids and tfs
	serialize_vector_u64(bloom_entry.topk_doc_ids, filename);
	serialize_vector_float(bloom_entry.topk_term_freqs, filename);
}

BloomEntry deserialize_bloom_entry(const char* filename) {
	BloomEntry bloom_entry;

	std::ifstream in_file(filename, std::ios::binary);
    if (!in_file) {
        std::cerr << "Error opening file for reading.\n";
        return bloom_entry;
    }

	// Read the size of the map
    size_t map_size;
    in_file.read(
			reinterpret_cast<char*>(&map_size), 
			sizeof(map_size)
			);

    // Read each key-value pair and insert it into the map
    for (size_t i = 0; i < map_size; ++i) {
		uint16_t tf;
        in_file.read(
				reinterpret_cast<char*>(&tf), 
				sizeof(uint16_t)
				);

		bloom_load(bloom_entry.bloom_filters[tf], filename);
    }

    in_file.close();

	deserialize_vector_u64(bloom_entry.topk_doc_ids, filename);
	deserialize_vector_float(bloom_entry.topk_term_freqs, filename);
}


// TODO: Fix saving and loading bloom filters. Need to read and write from offsets.
