#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <cmath>

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
		uint64_t &num_hashes,
		uint64_t &num_bits
		) {
	const double optimal_hash_count = -log2(fpr);
	num_hashes = round(optimal_hash_count);
	num_bits   = ceil((num_docs * log(fpr)) / log(1 / pow(2, log(2))));
}

BloomFilter init_bloom_filter(uint64_t max_entries, double fpr) {
	uint64_t num_hashes, num_bits;
	get_optimal_params(max_entries, fpr, num_hashes, num_bits);

	BloomFilter filter;
	filter.bits = (uint8_t*)calloc(num_bits / 8, sizeof(uint8_t));
	filter.num_bits = num_bits / 8;
	filter.seeds.reserve(num_hashes);

	for (uint64_t i = 0; i < num_hashes; ++i) {
		filter.seeds.push_back(rand());
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
		filter.bits[hash / 8] |= 1 << hash % 8;
	}
}

bool bloom_query(const BloomFilter& filter, const uint64_t key) {
	for (uint64_t i = 0; i < filter.seeds.size(); ++i) {
		uint64_t hash = fnv1a_64(key, filter.seeds[i]) % filter.num_bits;
		if (!(filter.bits[hash / 8] & 1 << hash % 8)) {
			return false;
		}
	}

	return true;
}

void bloom_clear(BloomFilter& filter) {
	memset(filter.bits, 0, filter.num_bits / 8);
}

void bloom_save(const BloomFilter& filter, const char* filename) {
	FILE* file = fopen(filename, "wb");
	fwrite(filter.bits, sizeof(uint8_t), filter.num_bits / 8, file);
	fclose(file);
}

void bloom_load(BloomFilter& filter, const char* filename) {
	FILE* file = fopen(filename, "rb");
	fread(filter.bits, sizeof(uint8_t), filter.num_bits / 8, file);
	fclose(file);
}
