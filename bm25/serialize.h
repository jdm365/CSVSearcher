#pragma once

#include <vector>

#include "engine.h"
#include "robin_hood.h"


void serialize_vector_u8(const std::vector<uint8_t>& vec, const std::string& filename);
void serialize_vector_u8(const std::vector<uint8_t>& vec, const std::ofstream& file);
void serialize_vector_u16(const std::vector<uint16_t>& vec, const std::string& filename);
void serialize_vector_u16(const std::vector<uint16_t>& vec, std::ofstream& file);
void serialize_vector_u32(const std::vector<uint32_t>& vec, const std::string& filename);
void serialize_vector_u64(const std::vector<uint64_t>& vec, const std::string& filename);
void serialize_vector_u64(const std::vector<uint64_t>& vec, std::ofstream& file);
void serialize_vector_float(const std::vector<float>& vec, const std::string& filename);
void serialize_vector_float(const std::vector<float>& vec, std::ofstream& file);
void serialize_inverted_index(const InvertedIndex& II, const std::string& filename);
void serialize_vector_of_vectors_u32(
		const std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		);
void serialize_vector_of_vectors_u64(
		const std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		);
void serialize_robin_hood_flat_map_string_u32(
		const robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		);
void serialize_robin_hood_flat_map_string_u64(
		const robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		);
void serialize_vector_of_vectors_pair_u32_u16(
		const std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		);
void serialize_vector_of_vectors_pair_u64_u16(
		const std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		);
void serialize_vector_of_vectors_u8(
		const std::vector<std::vector<uint8_t>>& vec, 
		const std::string& filename
		);


void deserialize_vector_u8(std::vector<uint8_t>& vec, const std::string& filename);
void deserialize_vector_u8(std::vector<uint8_t>& vec, std::ifstream& file);
void deserialize_vector_u16(std::vector<uint16_t>& vec, const std::string& filename);
void deserialize_vector_u16(std::vector<uint16_t>& vec, std::ifstream& file);
void deserialize_vector_u32(std::vector<uint32_t>& vec, const std::string& filename);
void deserialize_vector_u64(std::vector<uint64_t>& vec, const std::string& filename);
void deserialize_vector_u64(std::vector<uint64_t>& vec, std::ifstream& file);
void deserialize_vector_float(std::vector<float>& vec, const std::string& filename);
void deserialize_vector_float(std::vector<float>& vec, std::ifstream& file);
void deserialize_inverted_index(InvertedIndex& II, const std::string& filename);
void deserialize_vector_of_vectors_u32(
		std::vector<std::vector<uint32_t>>& vec, 
		const std::string& filename
		);
void deserialize_vector_of_vectors_u64(
		std::vector<std::vector<uint64_t>>& vec, 
		const std::string& filename
		);
void deserialize_robin_hood_flat_map_string_u32(
		robin_hood::unordered_flat_map<std::string, uint32_t>& map,
		const std::string& filename
		);
void deserialize_robin_hood_flat_map_string_u64(
		robin_hood::unordered_flat_map<std::string, uint64_t>& map,
		const std::string& filename
		);
void deserialize_vector_of_vectors_pair_u32_u16(
		std::vector<std::vector<std::pair<uint32_t, uint16_t>>>& vec, 
		const std::string& filename
		);
void deserialize_vector_of_vectors_pair_u64_u16(
		std::vector<std::vector<std::pair<uint64_t, uint16_t>>>& vec, 
		const std::string& filename
		);
void deserialize_vector_of_vector_u8(
		std::vector<std::vector<uint8_t>>& vec, 
		const std::string& filename
		);

void serialize_bloom_entry(
		const BloomEntry& bloom_entry,
		const char* filename
		);
BloomEntry deserialize_bloom_entry(const char* filename);
