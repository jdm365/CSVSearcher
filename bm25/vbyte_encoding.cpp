#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include <vector>

#include "vbyte_encoding.h"

void compress_uint32(
	uint32_t* data,
	uint8_t** compressed_buffer, 
	uint32_t raw_data_size,
	uint32_t* compressed_size
	) {
	// Use vbyte encoding

	*compressed_buffer = (uint8_t*)malloc(raw_data_size * sizeof(data[0]));
	*compressed_size = 0;

	for (uint32_t i = 0; i < raw_data_size; i++) {
		uint32_t value = data[i];
		uint8_t* buffer_ptr = *compressed_buffer + *compressed_size;

		while (value >= 128) {
			*buffer_ptr++ = (value & 127) | 128;
			value >>= 7;
			(*compressed_size)++;
		}
		*buffer_ptr++ = value;
		(*compressed_size)++;
	}

	// realloc to remove the extra space
	// *compressed_buffer = (uint8_t*)realloc(*compressed_buffer, *compressed_size);
}

void vbyte_decode_uint64(
	uint8_t*  compressed_value, 
	uint64_t* value
	) {
	*value = 0;
	uint64_t shift = 0;
	uint8_t byte;
	do {
		byte = *compressed_value++;
		*value |= (byte & 127) << shift;
		shift += 7;
	} while (byte & 128);
}

void decompress_uint32(
	uint8_t* compressed_buffer,
	uint32_t* data,
	uint32_t compressed_size
	) {
	uint32_t data_index = 0;
	uint32_t compressed_index = 0;

	while (compressed_index < compressed_size) {
		uint32_t value = 0;
		uint32_t shift = 0;
		uint8_t byte;
		do {
			byte = compressed_buffer[compressed_index++];
			value |= (byte & 127) << shift;
			shift += 7;
		} while (byte & 128);
		data[data_index++] = value;
	}
}

void compress_uint32_differential(
	uint32_t* data,
	uint8_t** compressed_buffer, 
	uint32_t raw_data_size,
	uint32_t* compressed_size
	) {
	// Convert to differential
	for (uint32_t i = raw_data_size - 1; i > 0; i--) {
		data[i] -= data[i - 1];
	}

	compress_uint32(data, compressed_buffer, raw_data_size, compressed_size);
}

void compress_uint64(
	uint64_t* data,
	uint8_t* compressed_buffer, 
	uint64_t raw_data_size,
	uint64_t* compressed_size
	) {
	// Use vbyte encoding
	// Assumes memory already allocated for compressed_buffer.
	*compressed_size = 0;

	for (uint64_t i = 0; i < raw_data_size; i++) {
		uint64_t value = data[i];
		uint8_t* buffer_ptr = compressed_buffer + *compressed_size;

		while (value >= 128) {
			*buffer_ptr++ = (value & 127) | 128;
			value >>= 7;
			(*compressed_size)++;
		}
		*buffer_ptr++ = value;
		(*compressed_size)++;
	}
}

void decompress_uint64(
	uint8_t* compressed_buffer,
	uint64_t* data,
	uint64_t compressed_size,
	uint64_t* decompressed_size
	) {
	uint64_t data_index = 0;
	uint64_t compressed_index = 0;

	while (compressed_index < compressed_size) {
		uint64_t value = 0;
		uint64_t shift = 0;
		uint8_t byte;
		do {
			byte = compressed_buffer[compressed_index++];
			value |= (byte & 127) << shift;
			shift += 7;
		} while (byte & 128);
		data[data_index++] = value;
	}
	*decompressed_size = data_index;
}


void compress_uint64_differential(
	uint64_t* data,
	uint8_t*  compressed_buffer, 
	uint64_t  raw_data_size,
	uint64_t* compressed_size
	) {
	// Convert to differential
	for (uint64_t i = raw_data_size - 1; i > 0; i--) {
		data[i] -= data[i - 1];
	}

	compress_uint64(data, compressed_buffer, raw_data_size, compressed_size);
}

void decompress_uint64_differential(
	uint8_t*  compressed_buffer,
	uint64_t* data,
	uint64_t  compressed_size,
	uint64_t* decompressed_size
	) {
	decompress_uint64(compressed_buffer, data, compressed_size, decompressed_size);

	// Convert back to original
	for (uint64_t i = 1; i < *decompressed_size; i++) {
		data[i] += data[i - 1];
	}
}

void vbyte_encode_uint64(
	uint64_t  value,
	uint8_t*  compressed_value, 
	uint64_t* compressed_size
	) {
	*compressed_size = 0;

	while (value >= 128) {
		*compressed_value++ = (value & 127) | 128;
		value >>= 7;
		(*compressed_size)++;
	}
	*compressed_value++ = value;
	(*compressed_size)++;
}

void compress_uint64(
	std::vector<uint64_t>& data,
	std::vector<uint8_t>&  compressed_buffer
	) {

	for (uint64_t i = 0; i < data.size(); ++i) {
		uint64_t value = data[i];

		while (value >= 128) {
			compressed_buffer.push_back((value & 127) | 128);
			value >>= 7;
		}
		compressed_buffer.push_back(value);
	}
}

void decompress_uint64(
	std::vector<uint8_t>&  compressed_buffer,
	std::vector<uint64_t>& data
	) {
	uint64_t compressed_index = 0;

	while (compressed_index < compressed_buffer.size()) {
		uint64_t value = 0;
		uint64_t shift = 0;
		uint8_t byte;
		do {
			byte = compressed_buffer[compressed_index++];
			value |= (byte & 127) << shift;
			shift += 7;
		} while (byte & 128);
		data.push_back(value);
	}
}

void decompress_uint64_partial(
	std::vector<uint8_t>&  compressed_buffer,
	std::vector<uint64_t>& data,
	uint32_t k
	) {
	uint64_t compressed_index = 0;

	while (compressed_index < compressed_buffer.size()) {
		uint64_t value = 0;
		uint64_t shift = 0;
		uint8_t byte;
		do {
			byte = compressed_buffer[compressed_index++];
			value |= (byte & 127) << shift;
			shift += 7;
		} while (byte & 128);
		data.push_back(value);

		if (data.size() == k) return;
	}
}

void compress_uint64_differential_single(
    std::vector<uint8_t>& data,
    uint64_t new_uncompressed_id,
	uint64_t prev_id
	) {
    uint64_t diff = new_uncompressed_id - prev_id;

    while (diff >= 128) {
        data.push_back((diff & 127) | 128);
        diff >>= 7;
    }

    // Push back the remaining diff value
    data.push_back(diff);
}

void compress_uint64_differential(
	std::vector<uint64_t>& data,
	std::vector<uint8_t>&  compressed_buffer
	) {
	uint64_t* copy_data = (uint64_t*)malloc(data.size() * sizeof(data[0]));
	memcpy(copy_data, data.data(), data.size() * sizeof(data[0]));

	uint64_t compressed_size;
	compress_uint64_differential(
		copy_data,
		compressed_buffer.data(),
		data.size(),
		&compressed_size
		);
}

void decompress_uint64_differential(
	std::vector<uint8_t>&  compressed_buffer,
	std::vector<uint64_t>& data
	) {
	uint64_t decompressed_size;
	decompress_uint64_differential(
		compressed_buffer.data(),
		data.data(),
		compressed_buffer.size(),
		&decompressed_size
		);
}
