#pragma once

#include <stdint.h>

#include <vector>

void compress_uint32(
	uint32_t* data,
	uint8_t** compressed_buffer, 
	uint32_t raw_data_size,
	uint32_t* compressed_size
	);

void decompress_uint32(
	uint8_t* compressed_buffer,
	uint32_t* data,
	uint32_t compressed_size
	);

void compress_uint32_differential(
	uint32_t* data,
	uint8_t** compressed_buffer, 
	uint32_t raw_data_size,
	uint32_t* compressed_size
	);

void decompress_uint32_differential(
	uint8_t* compressed_buffer,
	uint32_t* data,
	uint32_t compressed_size
	);

void compress_uint64(
	uint64_t* data,
	uint8_t*  compressed_buffer, 
	uint64_t  raw_data_size,
	uint64_t* compressed_size
	);

void decompress_uint64(
	uint8_t*  compressed_buffer,
	uint64_t* data,
	uint64_t  compressed_size,
	uint64_t* decompressed_size
	);

bool compress_uint64_differential_single(
	std::vector<uint8_t>& data,
	uint64_t new_uncompressed_id,
	uint64_t prev_id
	);

void compress_uint64_differential(
	uint64_t* data,
	uint8_t*  compressed_buffer, 
	uint64_t  raw_data_size,
	uint64_t* compressed_size
	);

void decompress_uint64_differential(
	uint8_t*  compressed_buffer,
	uint64_t* data,
	uint64_t  compressed_size,
	uint64_t* decompressed_size
	);

void vbyte_encode_uint64(
	uint64_t  value,
	uint8_t*  compressed_value, 
	uint64_t* compressed_size
	);

void vbyte_decode_uint64(
	uint8_t*  compressed_value, 
	uint64_t* value
	);

void compress_uint64(
	std::vector<uint64_t>& data,
	std::vector<uint8_t>&  compressed_buffer
	);

void decompress_uint64(
	std::vector<uint8_t>&  compressed_buffer,
	std::vector<uint64_t>& data
	);
