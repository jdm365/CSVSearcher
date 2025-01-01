const std = @import("std");
const TermPos = @import("server.zig").TermPos;

pub const TOKEN_STREAM_CAPACITY = 1_048_576;

pub const token_t = packed struct(u32) {
    new_doc: u1,
    term_pos: u7,
    doc_id: u24
};

const SIMD_QUOTE_MASK   = @as(@Vector(32, u8), @splat('"'));
const SIMD_NEWLINE_MASK = @as(@Vector(32, u8), @splat('\n'));
const SIMD_COMMA_MASK   = @as(@Vector(32, u8), @splat(','));

pub inline fn iterUTF8(read_buffer: []const u8, read_idx: *usize) u8 {
    // Return final byte.

    const byte: u8 = read_buffer[read_idx.*];
    const size: usize = @as(usize, @intFromBool(byte > 127)) 
                        + @as(usize, @intFromBool(byte > 223)) 
                        + @as(usize, @intFromBool(byte > 239));
    read_idx.* += size + @intFromBool(size == 0);

    return read_buffer[read_idx.* - 1];
}

pub inline fn readUTF8(
    read_buffer: []const u8,
    write_buffer: []u8,
    read_idx: *usize,
    write_idx: *usize,
    uppercase: bool,
) u8 {
    // Return final byte.
    // TODO: Add support multilingual delimiters.

    const byte: u8 = read_buffer[read_idx.*];
    const size: usize = @as(usize, @intFromBool(byte > 127)) 
                        + @as(usize, @intFromBool(byte > 223)) 
                        + @as(usize, @intFromBool(byte > 239));
    @memcpy(
        write_buffer[write_idx.*..write_idx.* + size + @intFromBool(size == 0)],
        read_buffer[read_idx.*..read_idx.* + size + @intFromBool(size == 0)],
    );
    read_idx.* += size + @intFromBool(size == 0);
    write_idx.* += size + @intFromBool(size == 0);

    if (uppercase) {
        write_buffer[write_idx.* - 1] = std.ascii.toUpper(write_buffer[write_idx.* - 1]);
    }

    return write_buffer[write_idx.* - 1];
}

pub inline fn _iterFieldCSV(buffer: []const u8, byte_idx: *usize) void {
    // Iterate to next field in compliance with RFC 4180.
    const is_quoted = buffer[byte_idx.*] == '"';
    byte_idx.* += @intFromBool(is_quoted);

    while (true) {
        byte_idx.* += 1;

        if (is_quoted) {
            // switch (iterUTF8(buffer, byte_idx)) {
            switch (buffer[byte_idx.* - 1]) {
                '"' => {
                    // Iter over delimeter or escape quote.
                    byte_idx.* += 1;

                    // Check escape quote.
                    if (buffer[byte_idx.* - 1] == '"') continue;
                    return;
                },
                else => {},
            }
        } else {
            // switch (iterUTF8(buffer, byte_idx)) {
            switch (buffer[byte_idx.* - 1]) {
                ',', '\n' => return,
                else => {},
            }
        }
    }
}


pub inline fn iterLineCSV(buffer: []const u8, byte_idx: *usize) void {
    // Iterate to next line in compliance with RFC 4180.
    while (true) {
        // switch (iterUTF8(buffer, byte_idx)) {
        byte_idx.* += 1;

        switch (buffer[byte_idx.* - 1]) {
            '"' => {
                while (true) {
                    // if (iterUTF8(buffer, byte_idx) == '"') {
                    byte_idx.* += 1;
                    if (buffer[byte_idx.* - 1] == '"') {
                        if (buffer[byte_idx.*] == '"') {
                            byte_idx.* += 1;
                            continue;
                        }
                        break;
                    }
                }
            },
            '\n' => {
                return;
            },
            else => {},
        }
    }
}

pub inline fn parseRecordCSV(
    buffer: []const u8,
    result_positions: []TermPos,
) !void {
    // Parse CSV record in compliance with RFC 4180.
    var byte_idx: usize = 0;
    for (0..result_positions.len) |idx| {
        const start_pos = byte_idx;
        _iterFieldCSV(buffer, &byte_idx);
        result_positions[idx] = TermPos{
            .start_pos = @as(u32, @intCast(start_pos)) + @intFromBool(buffer[start_pos] == '"'),
            .field_len = @as(u32, @intCast(byte_idx - start_pos - 1)) - @intFromBool(buffer[start_pos] == '"'),
        };
    }
}

pub const TokenStream = struct {
    tokens: []token_t,
    f_data: []align(std.mem.page_size) u8,
    num_terms: u32,
    allocator: std.mem.Allocator,
    output_file: std.fs.File,

    pub fn init(
        filename: []const u8,
        output_filename: []const u8,
        allocator: std.mem.Allocator
    ) !TokenStream {

        const file = try std.fs.cwd().openFile(filename, .{});
        const file_size = try file.getEndPos();

        const output_file = try std.fs.cwd().createFile(output_filename, .{ .read = true });

        const token_stream = TokenStream{
            .tokens = try allocator.alloc(token_t, TOKEN_STREAM_CAPACITY),
            .f_data = try std.posix.mmap(
                null,
                file_size,
                std.posix.PROT.READ,
                .{ .TYPE = .PRIVATE },
                file.handle,
                0
            ),
            .num_terms = 0,
            .allocator = allocator,
            .output_file = output_file,
        };

        return token_stream;
    }

    pub fn deinit(self: *TokenStream) void {
        std.posix.munmap(self.f_data);
        self.allocator.free(self.tokens);
        self.output_file.close();
    }
    
    pub fn addToken(
        self: *TokenStream,
        new_doc: bool,
        term_pos: u8,
        doc_id: u32,
    ) !void {
        self.tokens[self.num_terms] = token_t{
            .new_doc = @intFromBool(new_doc),
            .term_pos = @truncate(term_pos),
            .doc_id = @intCast(doc_id),
        };
        self.num_terms += 1;

        if (self.num_terms == TOKEN_STREAM_CAPACITY) {
            try self.flushTokenStream();
        }
    }

    pub inline fn flushTokenStream(self: *TokenStream) !void {
        const bytes_to_write = @sizeOf(u32) * self.num_terms;
        _ = try self.output_file.write(
            std.mem.asBytes(&self.num_terms),
            );
        const bytes_written = try self.output_file.write(
            std.mem.sliceAsBytes(self.tokens[0..self.num_terms])
            );
        
        std.debug.assert(bytes_written == bytes_to_write);

        self.num_terms = 0;
    }

    // pub inline fn iterFieldCSV(self: *TokenStream, byte_idx: *usize) !void {
        // // Iterate to next field in compliance with RFC 4180.
        // const is_quoted = self.f_data[byte_idx.*] == '"';
        // byte_idx.* += @intFromBool(is_quoted);
       //  
        // while (true) {
            // if (!iterUTF8(self.f_data, byte_idx)) continue;
            // const prev_byte_idx = byte_idx.* - 1;
// 
            // if (is_quoted) {
// 
                // if (self.f_data[prev_byte_idx] == '"') {
                    // // Iter over delimeter or escape quote.
                    // byte_idx.* += 1;
// 
                    // if (self.f_data[byte_idx.* - 1] == '"') continue;
                    // return;
                // }
// 
            // } else {
// 
                // switch (self.f_data[prev_byte_idx]) {
                    // ',', '\n' => return,
                    // else => {},
                // }
            // }
        // }
    // }
    pub inline fn iterFieldCSV(self: *TokenStream, byte_idx: *usize) void {
        // Iterate to next field in compliance with RFC 4180.
        _iterFieldCSV(self.f_data, byte_idx);
    }
};
