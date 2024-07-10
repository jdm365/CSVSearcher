#pragma once

#include <stdbool.h>
#include <stdint.h>

#include <vector>
#include <fstream>


typedef struct {
	std::vector<uint32_t> seeds;
	uint8_t* bits;
	size_t   num_bits;
} BloomFilter;

uint64_t fnv1a_64(uint64_t key, uint64_t seed);

void get_optimal_params(
		uint64_t num_docs,
		double fpr,
		uint64_t &num_hashes,
		uint64_t &num_bits
		);

BloomFilter init_bloom_filter(uint64_t max_entries, double fpr);
void bloom_free(BloomFilter& filter);
void bloom_put(BloomFilter& filter, const uint64_t key);
bool bloom_query(const BloomFilter& filter, const uint64_t key);
void bloom_clear(BloomFilter& filter);
void bloom_save(const BloomFilter& filter, const char* filename);
void bloom_load(BloomFilter& filter, const char* filename);
void bloom_save(const BloomFilter& filter, std::ofstream& file);
void bloom_load(BloomFilter& filter, std::ifstream& file);
