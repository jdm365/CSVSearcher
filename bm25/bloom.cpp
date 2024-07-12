#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <random>

#include <vector>

#include "bloom.h"


const uint64_t FNV_prime    = 1099511628211ULL;
const uint64_t offset_basis = 14695981039346656037ULL;

uint64_t fnv1a_64(uint64_t key, uint64_t seed) {
    uint64_t hash = offset_basis ^ seed;

    for (int i = 0; i < 8; ++i) {
        hash ^= (key & 0xFF);
        hash *= FNV_prime;
        key >>= 8;
    }

    return hash;
}

void get_optimal_params(
		uint64_t num_docs,
		double fpr,
		uint64_t& num_hashes,
		uint64_t& num_bits
		) {
	const double optimal_hash_count = -log2(fpr);
	num_hashes = round(optimal_hash_count);
	num_bits   = ceil((num_docs * log(fpr)) / log(1 / pow(2, log(2))));
}


BloomFilter init_bloom_filter(uint64_t max_entries, double fpr) {
    uint64_t num_hashes, num_bits;
    get_optimal_params(max_entries, fpr, num_hashes, num_bits);

    BloomFilter filter;
    filter.bits = (uint8_t*)calloc((num_bits + 7) / 8, sizeof(uint8_t));
    filter.num_bits = num_bits;
    filter.seeds.reserve(num_hashes);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;

    for (uint64_t i = 0; i < num_hashes; ++i) {
        filter.seeds.push_back(dis(gen));
    }

    return filter;
}

void bloom_free(BloomFilter& filter) {
	free(filter.bits);
	filter.bits = nullptr;
	filter.num_bits = 0;
	filter.seeds.clear();
}

void bloom_put(BloomFilter& filter, const uint64_t key) {
    for (uint64_t i = 0; i < filter.seeds.size(); ++i) {
        uint64_t hash = fnv1a_64(key, filter.seeds[i]) % filter.num_bits;
        filter.bits[hash / 8] |= 1 << (hash % 8);
    }
}

bool bloom_query(const BloomFilter& filter, const uint64_t key) {
    for (uint64_t i = 0; i < filter.seeds.size(); ++i) {
        uint64_t hash = fnv1a_64(key, filter.seeds[i]) % filter.num_bits;
        if (!(filter.bits[hash / 8] & (1 << (hash % 8)))) {
            return false;
        }
    }

    return true;
}

void bloom_clear(BloomFilter& filter) {
	memset(filter.bits, 0, (filter.num_bits + 7) / 8);
}

void bloom_save(const BloomFilter& filter, const char* filename) {
    FILE* file = fopen(filename, "wb");
    if (file) {
		// Write seeds
		uint64_t num_seeds = filter.seeds.size();
		fwrite(&num_seeds, sizeof(num_seeds), 1, file);
		fwrite(filter.seeds.data(), sizeof(uint32_t), num_seeds, file);

		// Write bits
		fwrite(&filter.num_bits, sizeof(filter.num_bits), 1, file);
		fwrite(filter.bits, sizeof(uint8_t), (filter.num_bits + 7) / 8, file);
		fclose(file);
    }
}

void bloom_load(BloomFilter& filter, const char* filename) {
    FILE* file = fopen(filename, "rb");
    if (file) {
		// Read seeds
		uint64_t num_seeds;
		size_t size = fread(&num_seeds, sizeof(num_seeds), 1, file);
		if (size != 1) {
			printf("Failed to read number of seeds\n");
			filter.seeds.clear();
			return;
		}

		filter.seeds.resize(num_seeds);
		size = fread(filter.seeds.data(), sizeof(uint32_t), num_seeds, file);
		if (size != num_seeds) {
			printf("Failed to read seeds\n");
			filter.seeds.clear();
			return;
		}

		// Read bits
		size = fread(&filter.num_bits, sizeof(filter.num_bits), 1, file);
		if (size != 1) {
			printf("Failed to read number of bits\n");
			filter.num_bits = 0;
			return;
		}

		filter.bits = (uint8_t*)calloc((filter.num_bits + 7) / 8, sizeof(uint8_t));
		size = fread(filter.bits, sizeof(uint8_t), (filter.num_bits + 7) / 8, file);
		if (size != (filter.num_bits + 7) / 8) {
			printf("Failed to read bits\n");
			free(filter.bits);
			filter.bits = nullptr;
			filter.num_bits = 0;
			return;
		}
    }
}

void bloom_save(const BloomFilter& filter, std::ofstream& file) {
	// Write seeds
	uint64_t num_seeds = filter.seeds.size();
	file.write((char*)&num_seeds, sizeof(num_seeds));

	file.write((char*)filter.seeds.data(), num_seeds * sizeof(uint32_t));

	// Write bits
	file.write((char*)&filter.num_bits, sizeof(filter.num_bits));
	file.write((char*)filter.bits, (filter.num_bits + 7) / 8);
}

void bloom_load(BloomFilter& filter, std::ifstream& file) {
	// Read seeds
	uint64_t num_seeds;
	file.read((char*)&num_seeds, sizeof(num_seeds));

	if (num_seeds > 256) {
		printf("Number of seeds is too large: %lu\n", num_seeds);
		filter.seeds.clear();

		std::exit(1);
		return;
	}

	filter.seeds.resize(num_seeds);
	file.read((char*)filter.seeds.data(), num_seeds * sizeof(uint32_t));

	// Read bits
	file.read((char*)&filter.num_bits, sizeof(filter.num_bits));

	filter.bits = (uint8_t*)calloc((filter.num_bits + 7) / 8, sizeof(uint8_t));
	file.read((char*)filter.bits, (filter.num_bits + 7) / 8);
}
