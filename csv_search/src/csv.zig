const std = @import("std");
const TermPos = @import("server.zig").TermPos;

pub const TOKEN_STREAM_CAPACITY = 1_048_576;

pub const token_t = packed struct(u32) {
    new_doc: u1,
    term_pos: u7,
    doc_id: u24
};

pub fn iterFieldCSV(buffer: []const u8, byte_pos: *usize) !void {
    // Iterate to next field in compliance with RFC 4180.
    const is_quoted = buffer[byte_pos.*] == '"';
    byte_pos.* += @intFromBool(is_quoted);

    while (true) {

        if (buffer[byte_pos.*] > 127) {
            while (buffer[byte_pos.*] > 127) {
                byte_pos.* += 1;
            }

            byte_pos.* += 1;
            continue;
        }

        if (is_quoted) {

            if (buffer[byte_pos.*] == '"') {
                byte_pos.* += 2;
                if (buffer[byte_pos.* - 1] != '"') {
                    return;
                }
            } else {
                byte_pos.* += 1;
            }

        } else {

            switch (buffer[byte_pos.*]) {
                ',' => {
                    byte_pos.* += 1;
                    return;
                },
                '\n' => {
                    byte_pos.* += 1;
                    return;
                },
                else => {
                    byte_pos.* += 1;
                }
            }
        }
    }
}

pub fn parseRecordCSV(
    buffer: []const u8,
    result_positions: []TermPos,
) !void {
    // Parse CSV record in compliance with RFC 4180.
    var byte_pos: usize = 0;
    for (0..result_positions.len) |idx| {
        const start_pos = byte_pos;
        try iterFieldCSV(buffer, &byte_pos);
        result_positions[idx] = TermPos{
            .start_pos = @as(u32, @intCast(start_pos)) + @intFromBool(buffer[start_pos] == '"'),
            .field_len = @as(u32, @intCast(byte_pos - start_pos - 1)) - @intFromBool(buffer[start_pos] == '"'),
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

    pub fn flushTokenStream(self: *TokenStream) !void {
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

    pub fn iterFieldCSV(self: *TokenStream, byte_pos: *usize) !void {
        // Iterate to next field in compliance with RFC 4180.
        const is_quoted = self.f_data[byte_pos.*] == '"';
        byte_pos.* += @intFromBool(is_quoted);
        
        while (true) {
            if (self.f_data[byte_pos.*] > 127) {
                while (self.f_data[byte_pos.*] > 127) {
                    byte_pos.* += 1;
                }

                byte_pos.* += 1;
                continue;
            }

            if (is_quoted) {

                if (self.f_data[byte_pos.*] == '"') {
                    byte_pos.* += 2;
                    if (self.f_data[byte_pos.* - 1] != '"') {
                        return;
                    }
                } else {
                    byte_pos.* += 1;
                }

            } else {

                switch (self.f_data[byte_pos.*]) {
                    ',' => {
                        byte_pos.* += 1;
                        return;
                    },
                    '\n' => {
                        byte_pos.* += 1;
                        return;
                    },
                    else => {
                        byte_pos.* += 1;
                    }
                }
            }
        }
    }
};


