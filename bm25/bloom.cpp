#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <cmath>
#include <random>

#include <vector>

#include "bloom.h"

#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))


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

uint64_t get_bloom_memory_usage(const BloomFilter& filter) {
	return (filter.num_bits + 7) / 8;
}


ChunkedBloomFilter init_chunked_bloom_filter(uint32_t max_elements_chunk, double fpr) {
	ChunkedBloomFilter filter;
	filter.num_filters        = 1;
	filter.num_elements_chunk = 0; 
	filter.max_elements_chunk = max_elements_chunk; 

	uint64_t num_hashes, num_bits;
	get_optimal_params(max_elements_chunk, fpr, num_hashes, num_bits);
	num_bits = max(num_bits, 8);

	filter.num_hashes = num_hashes;
	filter.bits    = (uint8_t**)calloc(filter.num_filters, sizeof(uint8_t*));
	filter.bits[0] = (uint8_t*)calloc((num_bits + 7) / 8, sizeof(uint8_t));
	filter.num_bits_chunk = num_bits;

	assert(num_bits > 7);
	assert(num_hashes > 0); 
	assert(num_hashes <= 128);

	return filter;
}

void bloom_free(ChunkedBloomFilter& filter) {
	for (uint64_t i = 0; i < filter.num_filters; ++i) {
		free(filter.bits[i]);
	}

	free(filter.bits);
	filter.bits = nullptr;
	filter.num_bits_chunk = 0;
	filter.num_filters    = 0;
	filter.num_hashes     = 0;
	filter.num_elements_chunk = 0;
}

void bloom_put(ChunkedBloomFilter& filter, const uint64_t key) {
	for (uint64_t i = 0; i < filter.num_hashes; ++i) {
		uint64_t hash = fnv1a_64(key, SEEDS[i]) % filter.num_bits_chunk;
		filter.bits[filter.num_filters - 1][hash / 8] |= 1 << (hash % 8);
	}

	if (++filter.num_elements_chunk == filter.max_elements_chunk) {
		// Allocate new filter
		++filter.num_filters;
		filter.bits = (uint8_t**)realloc(filter.bits, filter.num_filters * sizeof(uint8_t*));
		filter.bits[filter.num_filters - 1] = (uint8_t*)calloc((filter.num_bits_chunk + 7) / 8, sizeof(uint8_t));
		filter.num_elements_chunk = 0;
	}
}

bool bloom_query(const ChunkedBloomFilter& filter, const uint64_t key) {
	// Precompute hashes
	uint64_t hashes[128];
	for (uint64_t i = 0; i < filter.num_hashes; ++i) {
		hashes[i] = fnv1a_64(key, filter.num_hashes) % filter.num_bits_chunk;
	}

	for (uint64_t i = 0; i < filter.num_filters; ++i) {
		bool found = true;
		for (uint64_t j = 0; j < filter.num_hashes; ++j) {
			uint64_t hash = hashes[j];
			if (!(filter.bits[i][hash / 8] & (1 << (hash % 8)))) {
				found = false;
				break;
			}
		}

		if (found) return true;
	}

	return false;
}

void bloom_clear(ChunkedBloomFilter& filter) {
	for (uint64_t i = 0; i < filter.num_filters; ++i) {
		memset(filter.bits[i], 0, (filter.num_bits_chunk + 7) / 8);
	}
}

void bloom_save(const ChunkedBloomFilter& filter, const char* filename) {
	FILE* file = fopen(filename, "wb");
	if (file) {
		// Write number of filters
		fwrite(&filter.num_filters, sizeof(filter.num_filters), 1, file);

		// Write number of bits per chunk
		fwrite(&filter.num_bits_chunk, sizeof(filter.num_bits_chunk), 1, file);

		// Write number of hashes
		fwrite(&filter.num_hashes, sizeof(filter.num_hashes), 1, file);

		// Write max elements per chunk
		fwrite(&filter.max_elements_chunk, sizeof(filter.max_elements_chunk), 1, file);

		// Write elements per chunk
		fwrite(&filter.num_elements_chunk, sizeof(filter.num_elements_chunk), 1, file);

		// Write bits
		for (uint64_t i = 0; i < filter.num_filters; ++i) {
			fwrite(filter.bits[i], sizeof(uint8_t), (filter.num_bits_chunk + 7) / 8, file);
		}

		fclose(file);
	}
}

void bloom_load(ChunkedBloomFilter& filter, const char* filename) {
	FILE* file = fopen(filename, "rb");
	if (file) {
		// Read number of filters
		size_t size = fread(&filter.num_filters, sizeof(filter.num_filters), 1, file);
		if (size != 1) {
			printf("Failed to read number of filters\n");
			filter.num_filters = 0;
			return;
		}

		// Read number of bits per chunk
		size = fread(&filter.num_bits_chunk, sizeof(filter.num_bits_chunk), 1, file);
		if (size != 1) {
			printf("Failed to read number of bits per chunk\n");
			filter.num_bits_chunk = 0;
			return;
		}

		// Read number of hashes
		size = fread(&filter.num_hashes, sizeof(filter.num_hashes), 1, file);
		if (size != 1) {
			printf("Failed to read number of hashes\n");
			filter.num_hashes = 0;
			return;
		}

		// Read max elements per chunk
		size = fread(&filter.max_elements_chunk, sizeof(filter.max_elements_chunk), 1, file);
		if (size != 1) {
			printf("Failed to read max elements per chunk\n");
			filter.max_elements_chunk = 0;
			return;
		}

		// Read elements per chunk
		size = fread(&filter.num_elements_chunk, sizeof(filter.num_elements_chunk), 1, file);
		if (size != 1) {
			printf("Failed to read elements per chunk\n");
			filter.num_elements_chunk = 0;
			return;
		}

		// Read bits
		filter.bits = (uint8_t**)calloc(filter.num_filters, sizeof(uint8_t*));
		for (uint64_t i = 0; i < filter.num_filters; ++i) {
			filter.bits[i] = (uint8_t*)calloc((filter.num_bits_chunk + 7) / 8, sizeof(uint8_t));
			size = fread(filter.bits[i], sizeof(uint8_t), (filter.num_bits_chunk + 7) / 8, file);
			if (size != (filter.num_bits_chunk + 7) / 8) {
				printf("Failed to read bits\n");
				free(filter.bits[i]);
				filter.bits[i] = nullptr;
				filter.num_bits_chunk = 0;
				return;
			}
		}

		fclose(file);
	}
}

void bloom_save(const ChunkedBloomFilter& filter, std::ofstream& file) {
	// Write number of filters
	file.write((char*)&filter.num_filters, sizeof(filter.num_filters));

	// Write number of bits per chunk
	file.write((char*)&filter.num_bits_chunk, sizeof(filter.num_bits_chunk));

	// Write number of hashes
	file.write((char*)&filter.num_hashes, sizeof(filter.num_hashes));

	// Write max elements per chunk
	file.write((char*)&filter.max_elements_chunk, sizeof(filter.max_elements_chunk));

	// Write elements per chunk
	file.write((char*)&filter.num_elements_chunk, sizeof(filter.num_elements_chunk));

	// Write bits
	for (uint64_t i = 0; i < filter.num_filters; ++i) {
		file.write((char*)filter.bits[i], (filter.num_bits_chunk + 7) / 8);
	}
}

void bloom_load(ChunkedBloomFilter& filter, std::ifstream& file) {
	// Read number of filters
	file.read((char*)&filter.num_filters, sizeof(filter.num_filters));

	// Read number of bits per chunk
	file.read((char*)&filter.num_bits_chunk, sizeof(filter.num_bits_chunk));

	// Read number of hashes
	file.read((char*)&filter.num_hashes, sizeof(filter.num_hashes));

	// Read max elements per chunk
	file.read((char*)&filter.max_elements_chunk, sizeof(filter.max_elements_chunk));

	// Read elements per chunk
	file.read((char*)&filter.num_elements_chunk, sizeof(filter.num_elements_chunk));

	// Read bits
	filter.bits = (uint8_t**)calloc(filter.num_filters, sizeof(uint8_t*));
	for (uint64_t i = 0; i < filter.num_filters; ++i) {
		filter.bits[i] = (uint8_t*)calloc((filter.num_bits_chunk + 7) / 8, sizeof(uint8_t));
		file.read((char*)filter.bits[i], (filter.num_bits_chunk + 7) / 8);
	}
}

uint64_t get_bloom_memory_usage(const ChunkedBloomFilter& filter) {
	uint64_t total = 0;
	for (uint64_t i = 0; i < filter.num_filters; ++i) {
		total += (filter.num_bits_chunk + 7) / 8;
	}

	return total;
}
