const std = @import("std");
const progress = @import("progress.zig");
const sorted_array = @import("sorted_array.zig");
const builtin = @import("builtin");
const TermPos = @import("server.zig").TermPos;
const csvLineToJson = @import("server.zig").csvLineToJson;
const zap = @import("zap");

const TOKEN_STREAM_CAPACITY = 1_048_576;
const MAX_LINE_LENGTH       = 1_048_576;
const MAX_NUM_TERMS         = 4096;
const MAX_TERM_LENGTH       = 64;
const MAX_NUM_RESULTS       = 1000;

const AtomicCounter = std.atomic.Value(u64);
const token_t = packed struct(u32) {
    new_doc: u1,
    term_pos: u7,
    doc_id: u24
};

const QueryResult = struct {
    doc_id: u32,
    score: f32,
    partition_idx: usize,
};

const Column = struct {
    csv_idx: usize,
    II_idx: usize,
};

const ColTokenPair = struct {
    col_idx: u32,
    token: u32,
};

pub fn StaticIntegerSet(comptime n: u32) type {
    return struct {
        const Self = @This();

        values: [n]u32,
        count: usize,

        pub fn init() Self {
            return Self{
                .values = undefined,
                .count = 0,
            };
        }

        pub fn clear(self: *Self) void {
            self.count = 0;
        }

        pub fn checkOrInsert(self: *Self, new_value: u32) bool {
            // TODO: Explore SIMD implementation. Expand copy new value into simd register LANE_WIDTH / 32 times. 
            //
            //
            // Don't allow new insertions if full.
            if (self.count == n) return true;

            // If element already exists return true, else return false.
            // If element doesn't exist also insert.
            for (0..self.count) |idx| {
                if (self.values[idx] == new_value) return true;
            }

            self.values[self.count] = new_value;
            self.count += 1;
            return false;
        }
    };
}

pub fn iterField(buffer: []const u8, byte_pos: *usize) !void {
    // Iterate to next field in compliance with RFC 4180.
    const is_quoted = buffer[byte_pos.*] == '"';
    byte_pos.* += @intFromBool(is_quoted);

    while (true) {
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
        try iterField(buffer, &byte_pos);
        result_positions[idx] = TermPos{
            .start_pos = @intCast(start_pos),
            .field_len = @intCast(byte_pos - start_pos - 1),
        };
    }
}

const TokenStream = struct {
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

    pub fn iterField(self: *TokenStream, byte_pos: *usize) !void {
        // Iterate to next field in compliance with RFC 4180.
        const is_quoted = self.f_data[byte_pos.*] == '"';
        byte_pos.* += @intFromBool(is_quoted);

        while (true) {
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
                        return error.UnexpectedNewline;
                    },
                    else => {
                        byte_pos.* += 1;
                    }
                }
            }
        }
    }
};


const InvertedIndex = struct {
    postings: []token_t,
    vocab: std.StringHashMap(u32),
    term_offsets: []u32,
    doc_freqs: std.ArrayList(u32),
    doc_sizes: []u16,

    num_terms: u32,
    num_docs: u32,
    avg_doc_size: f32,

    pub fn init(
        allocator: std.mem.Allocator,
        num_docs: usize,
        ) !InvertedIndex {
        var vocab = std.StringHashMap(u32).init(allocator);

        // Guess capacity
        try vocab.ensureTotalCapacity(@intCast(num_docs / 25));

        const II = InvertedIndex{
            .postings = &[_]token_t{},
            .vocab = vocab,
            .term_offsets = &[_]u32{},
            .doc_freqs = try std.ArrayList(u32).initCapacity(
                allocator, @as(usize, @intFromFloat(@as(f32, @floatFromInt(num_docs)) * 0.1))
                ),
            .doc_sizes = try allocator.alloc(u16, num_docs),
            .num_terms = 0,
            .num_docs = @intCast(num_docs),
            .avg_doc_size = 0.0,
        };
        @memset(II.doc_sizes, 0);
        return II;
    }

    pub fn deinit(
        self: *InvertedIndex,
        allocator: std.mem.Allocator,
        ) void {
        allocator.free(self.postings);
        self.vocab.deinit();

        allocator.free(self.term_offsets);
        self.doc_freqs.deinit();
        allocator.free(self.doc_sizes);
    }

    pub fn resizePostings(
        self: *InvertedIndex,
        allocator: std.mem.Allocator,
        ) !void {
        self.num_terms = @intCast(self.doc_freqs.items.len);
        self.term_offsets = try allocator.alloc(u32, self.num_terms);

        // Num terms is now known.
        var postings_size: usize = 0;
        for (0.., self.doc_freqs.items) |i, doc_freq| {
            self.term_offsets[i] = @intCast(postings_size);
            postings_size += doc_freq;
        }
        self.term_offsets[self.num_terms - 1] = @intCast(postings_size);
        self.postings = try allocator.alloc(token_t, postings_size + 1);

        var avg_doc_size: f64 = 0.0;
        for (self.doc_sizes) |doc_size| {
            avg_doc_size += @floatFromInt(doc_size);
        }
        avg_doc_size /= @floatFromInt(self.num_docs);
        self.avg_doc_size = @floatCast(avg_doc_size);
    }
};

const BM25Partition = struct {
    II: []InvertedIndex,
    line_offsets: []usize,
    allocator: std.mem.Allocator,
    string_arena: std.heap.ArenaAllocator,

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        line_offsets: []usize,
    ) !BM25Partition {
        const partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndex, num_search_cols),
            .line_offsets = line_offsets,
            .allocator = allocator,
            .string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };

        for (0..num_search_cols) |idx| {
            partition.II[idx] = try InvertedIndex.init(allocator, line_offsets.len - 1);
        }

        return partition;
    }

    pub fn deinit(self: *BM25Partition) void {
        self.allocator.free(self.line_offsets);
        for (0..self.II.len) |i| {
            self.II[i].deinit(self.allocator);
        }
        self.allocator.free(self.II);
        self.string_arena.deinit();
    }

    fn addTerm(
        self: *BM25Partition,
        term: *[MAX_TERM_LENGTH]u8,
        term_len: usize,
        doc_id: u32,
        term_pos: u8,
        col_idx: usize,
        token_stream: *TokenStream,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {

        const gop = try self.II[col_idx].vocab.getOrPut(term[0..term_len]);

        if (!gop.found_existing) {
            const term_copy = try self.string_arena.allocator().dupe(u8, term[0..term_len]);

            gop.key_ptr.* = term_copy;
            gop.value_ptr.* = self.II[col_idx].num_terms;
            self.II[col_idx].num_terms += 1;
            try self.II[col_idx].doc_freqs.append(1);
            try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*);
        } else {
            // std.debug.print("Value address: {*}\n", .{gop.value_ptr});
            // std.debug.print("Entry address: {*}\n", .{&gop});
            // std.debug.print("Offset: {}\n", .{@intFromPtr(gop.value_ptr) - @intFromPtr(&gop)}); 
            // @breakpoint();

            if (!terms_seen.checkOrInsert(gop.value_ptr.*)) {
                self.II[col_idx].doc_freqs.items[gop.value_ptr.*] += 1;
                try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*);
            }
        }

        self.II[col_idx].doc_sizes[doc_id] += 1;
        new_doc.* = false;
    }


    pub fn processDocRfc4180(
        self: *BM25Partition,
        token_stream: *TokenStream,
        doc_id: u32,
        byte_idx: *usize,
        col_idx: usize,
        term: *[MAX_TERM_LENGTH]u8,
        max_byte: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        var term_pos: u8 = 0;
        const is_quoted  = (token_stream.f_data[byte_idx.*] == '"');
        byte_idx.* += @intFromBool(is_quoted);

        var cntr: usize = 0;
        var new_doc: bool = (doc_id != 0);

        terms_seen.clear();

        if (is_quoted) {

            while (true) {
                std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);
                std.debug.assert(byte_idx.* < max_byte);

                if (token_stream.f_data[byte_idx.*] == '"') {
                    byte_idx.* += 1;

                    if ((token_stream.f_data[byte_idx.*] == ',') or (token_stream.f_data[byte_idx.*] == '\n')) {
                        break;
                    }

                    // Double quote means escaped quote. Opt not to include in token for now.
                    if (token_stream.f_data[byte_idx.*] == '"') {
                        byte_idx.* += 1;
                        continue;
                    }
                }

                if ((token_stream.f_data[byte_idx.*] == ' ') or (cntr == MAX_TERM_LENGTH - 1)) {
                    if (cntr == 0) {
                        byte_idx.* += 1;
                        continue;
                    }

                    try self.addTerm(
                        term, 
                        cntr, 
                        doc_id, 
                        term_pos, 
                        col_idx, 
                        token_stream, 
                        terms_seen,
                        &new_doc,
                        );

                    term_pos += @intFromBool(term_pos != 255);
                    cntr = 0;
                    byte_idx.* += 1;
                    continue;
                }

                term[cntr] = std.ascii.toUpper(token_stream.f_data[byte_idx.*]);
                cntr += 1;
                byte_idx.* += 1;
            }

        } else {

            while (true) {
                std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);
                std.debug.assert(byte_idx.* < max_byte);

                switch (token_stream.f_data[byte_idx.*]) {
                    ',' => break,
                    '\n' => break,
                    ' ' => {
                        if (cntr == 0) {
                            byte_idx.* += 1;
                            continue;
                        }

                        try self.addTerm(
                            term, 
                            cntr, 
                            doc_id, 
                            term_pos, 
                            col_idx, 
                            token_stream, 
                            terms_seen,
                            &new_doc,
                            );

                        term_pos += @intFromBool(term_pos != 255);
                        cntr = 0;
                        byte_idx.* += 1;
                    },
                    else => {
                        if (cntr == MAX_TERM_LENGTH - 1) {
                            try self.addTerm(
                                term, 
                                cntr, 
                                doc_id, 
                                term_pos, 
                                col_idx, 
                                token_stream, 
                                terms_seen,
                                &new_doc,
                                );

                            term_pos += @intFromBool(term_pos != 255);
                            cntr = 0;
                            byte_idx.* += 1;
                            continue;
                        }
                        term[cntr] = std.ascii.toUpper(token_stream.f_data[byte_idx.*]);
                        cntr += 1;
                        byte_idx.* += 1;
                    }
                }
            }
        }

        if (cntr > 0) {
            std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

            try self.addTerm(
                term, 
                cntr, 
                doc_id, 
                term_pos, 
                col_idx, 
                token_stream, 
                terms_seen,
                &new_doc,
                );
        }

        if (new_doc) {
            // No terms found. Add null token.
            try token_stream.addToken(
                true,
                std.math.maxInt(u7),
                std.math.maxInt(u24),
            );
        }


        byte_idx.* += 1;
    }


    pub fn constructFromTokenStream(
        self: *BM25Partition,
        token_streams: *[]TokenStream,
        ) !void {

        for (0.., self.II) |col_idx, *II| {
            try II.resizePostings(self.allocator);
            var term_offsets = try self.allocator.alloc(usize, II.num_terms);
            defer self.allocator.free(term_offsets);
            @memset(term_offsets, 0);

            // Create index.
            const ts = token_streams.*[col_idx];
            try ts.output_file.seekTo(0);

            var bytes_read: usize = 0;

            var num_tokens: usize = TOKEN_STREAM_CAPACITY;
            var current_doc_id: usize = 0;

            while (num_tokens == TOKEN_STREAM_CAPACITY) {
                var _num_tokens: [4]u8 = undefined;
                _ = try ts.output_file.read(std.mem.asBytes(&_num_tokens));
                const endianness = builtin.cpu.arch.endian();
                num_tokens = std.mem.readInt(u32, &_num_tokens, endianness);

                bytes_read = try ts.output_file.read(
                    std.mem.sliceAsBytes(ts.tokens[0..num_tokens])
                    );
                std.debug.assert(bytes_read == 4 * num_tokens);

                var token_count: usize = 0;
                for (token_count..token_count + num_tokens) |idx| {
                    if (@as(*u32, @ptrCast(&ts.tokens[idx])).* == std.math.maxInt(u32)) {
                        // Null token.
                        current_doc_id += 1;
                        continue;
                    }

                    const new_doc  = ts.tokens[idx].new_doc;
                    const term_pos = ts.tokens[idx].term_pos;
                    const term_id: usize = @intCast(ts.tokens[idx].doc_id);


                    current_doc_id += @intCast(new_doc);

                    const token = token_t{
                        .new_doc = 0,
                        .term_pos = term_pos,
                        .doc_id = @truncate(current_doc_id),
                    };

                    const postings_offset = II.term_offsets[term_id] + term_offsets[term_id];
                    std.debug.assert(postings_offset < II.postings.len);
                    std.debug.assert(current_doc_id < II.num_docs);

                    term_offsets[term_id] += 1;

                    II.postings[postings_offset] = token;
                }

                token_count += num_tokens;
            }
        }
    }

    pub fn fetchRecords(
        self: *const BM25Partition,
        result_positions: []TermPos,
        file_handle: *std.fs.File,
        query_result: QueryResult,
        record_string: *std.ArrayList(u8),
    ) !void {
        const doc_id: usize = @intCast(query_result.doc_id);
        const byte_offset = self.line_offsets[doc_id];
        const next_byte_offset = self.line_offsets[doc_id + 1];
        const bytes_to_read = next_byte_offset - byte_offset;

        std.debug.assert(bytes_to_read < MAX_LINE_LENGTH);

        try file_handle.seekTo(byte_offset);
        if (bytes_to_read > record_string.capacity) {
            try record_string.resize(bytes_to_read);
        }
        _ = try file_handle.read(record_string.items[0..bytes_to_read]);
        try parseRecordCSV(record_string.items[0..bytes_to_read], result_positions);
    }
};



pub const IndexManager = struct {
    index_partitions: []BM25Partition,
    input_filename: []const u8,
    allocator: std.mem.Allocator,
    string_arena: std.heap.ArenaAllocator,
    search_cols: std.StringHashMap(Column),
    cols: std.ArrayList([]const u8),
    file_handles: []std.fs.File,
    tmp_dir: []const u8,
    result_positions: [MAX_NUM_RESULTS][]TermPos,
    result_strings: [MAX_NUM_RESULTS]std.ArrayList(u8),
    results_arrays: []sorted_array.SortedScoreArray(QueryResult),
    thread_pool: std.Thread.Pool,
    header_bytes: usize,

    pub fn init(
        input_filename: []const u8,
        search_cols: *std.ArrayList([]u8),
        allocator: std.mem.Allocator,
        ) !IndexManager {

        var string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        var cols = std.ArrayList([]const u8).init(allocator);

        var header_bytes: usize = 0;

        const search_col_map = try readCSVHeader(
            input_filename, 
            search_cols, 
            &cols,
            allocator,
            string_arena.allocator(),
            &header_bytes,
            );


        const file_hash = blk: {
            var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(input_filename, &hash, .{});
            break :blk hash;
        };
        const dir_name = try std.fmt.allocPrint(
            allocator,
            ".{x:0>32}", .{std.fmt.fmtSliceHexLower(file_hash[0..16])}
            );

        std.fs.cwd().makeDir(dir_name) catch {
            try std.fs.cwd().deleteTree(dir_name);
            try std.fs.cwd().makeDir(dir_name);
        };

        std.debug.assert(cols.items.len > 0);

        var result_positions: [MAX_NUM_RESULTS][]TermPos = undefined;
        var result_strings: [MAX_NUM_RESULTS]std.ArrayList(u8) = undefined;
        for (0..MAX_NUM_RESULTS) |idx| {
            result_positions[idx] = try allocator.alloc(TermPos, cols.items.len);
            result_strings[idx] = try std.ArrayList(u8).initCapacity(allocator, 4096);
            try result_strings[idx].resize(4096);
        }

        return IndexManager{
            .index_partitions = undefined,
            .input_filename = input_filename,
            .allocator = allocator,
            .string_arena = string_arena,
            .search_cols = search_col_map,
            .cols = cols,
            .tmp_dir = dir_name,
            .file_handles = undefined,
            .result_positions = result_positions,
            .result_strings = result_strings,
            .results_arrays = undefined,
            .header_bytes = header_bytes,
        };
    }
    
    pub fn deinit(self: *IndexManager) !void {
        for (0..self.index_partitions.len) |idx| {
            self.index_partitions[idx].deinit();
            self.file_handles[idx].close();
        }
        self.allocator.free(self.file_handles);
        self.allocator.free(self.index_partitions);

        self.search_cols.deinit();
        self.cols.deinit();

        try std.fs.cwd().deleteTree(self.tmp_dir);
        self.allocator.free(self.tmp_dir);

        for (0..MAX_NUM_RESULTS) |idx| {
            self.allocator.free(self.result_positions[idx]);
            self.result_strings[idx].deinit();
            self.results_arrays[idx].deinit();
        }

        self.string_arena.deinit();
    }

    fn printDebugInfo(self: *const IndexManager) void {
        std.debug.print("\n=====================================================\n", .{});

        for (0..self.index_partitions.len) |idx| {
            std.debug.print("Partition {d}\n", .{idx});
            std.debug.print("---------------------------------------\n", .{});

            for (0..self.index_partitions[idx].II.len) |jdx| {
                const II = self.index_partitions[idx].II[jdx];

                std.debug.print("Column {d}\n", .{jdx});
                std.debug.print("Num terms: {d}\n", .{II.num_terms});
                std.debug.print("Num docs: {d}\n", .{II.num_docs});
                std.debug.print("Avg doc size: {d}\n\n", .{II.avg_doc_size});
            }
        }
        std.debug.print("=====================================================\n\n\n\n", .{});
    }


    fn readCSVHeader(
        input_filename: []const u8,
        search_cols: *std.ArrayList([]u8),
        cols: *std.ArrayList([]const u8),
        allocator: std.mem.Allocator,
        string_arena: std.mem.Allocator,
        num_bytes: *usize,
        ) !std.StringHashMap(Column) {

        var col_map = std.StringHashMap(Column).init(allocator);
        var col_idx: usize = 0;

        const file = try std.fs.cwd().openFile(input_filename, .{});
        defer file.close();

        const file_size = try file.getEndPos();

        const f_data = try std.posix.mmap(
            null,
            file_size,
            std.posix.PROT.READ,
            .{ .TYPE = .PRIVATE },
            file.handle,
            0
        );
        defer std.posix.munmap(f_data);

        var term: [MAX_TERM_LENGTH]u8 = undefined;
        var cntr: usize = 0;

        for (search_cols.items) |*col| {
            _ = std.ascii.upperString(col.*, col.*);
        }


        var byte_pos: usize = 0;
        line: while (true) {
            const is_quoted = f_data[byte_pos] == '"';
            byte_pos += @intFromBool(is_quoted);

            while (true) {
                if (is_quoted) {

                    if (f_data[byte_pos] == '"') {
                        byte_pos += 2;
                        if (f_data[byte_pos - 1] != '"') {
                            // ADD TERM
                            try cols.append(
                                try string_arena.dupe(u8, term[0..cntr])
                                );

                            for (0..search_cols.items.len) |idx| {
                                if (std.mem.eql(u8, term[0..cntr], search_cols.items[idx])) {
                                    std.debug.print("Found search col: {s}\n", .{search_cols.items[idx]});
                                    const copy_term = try string_arena.dupe(u8, term[0..cntr]);
                                    try col_map.put(
                                        copy_term, 
                                        Column{
                                            .csv_idx = col_idx,
                                            .II_idx = col_map.count(),
                                            },
                                        );
                                    break;
                                }
                            }
                            cntr = 0;
                            byte_pos += 1;
                            col_idx += 1;
                            continue :line;
                        }
                    } else {
                        // Add char.
                        term[cntr] = std.ascii.toUpper(f_data[byte_pos]);
                        cntr += 1;
                        byte_pos += 1;
                    }

                } else {

                    switch (f_data[byte_pos]) {
                        ',' => {
                            // ADD TERM.
                            try cols.append(
                                try string_arena.dupe(u8, term[0..cntr])
                                );

                            for (0..search_cols.items.len) |idx| {
                                if (std.mem.eql(u8, term[0..cntr], search_cols.items[idx])) {
                                    std.debug.print("Found search col: {s}\n", .{search_cols.items[idx]});
                                    const copy_term = try string_arena.dupe(u8, term[0..cntr]);
                                    try col_map.put(
                                        copy_term, 
                                        Column{
                                            .csv_idx = col_idx,
                                            .II_idx = col_map.count(),
                                            },
                                        );
                                    break;
                                }
                            }
                            cntr = 0;
                            byte_pos += 1;
                            col_idx += 1;
                            continue :line;
                        },
                        '\n' => {
                            // ADD TERM.
                            try cols.append(
                                try string_arena.dupe(u8, term[0..cntr])
                                );
                            num_bytes.* = byte_pos + 1;
                            return col_map;
                        },
                        else => {
                            // Add char
                            term[cntr] = std.ascii.toUpper(f_data[byte_pos]);
                            cntr += 1;
                            byte_pos += 1;
                        }
                    }
                }
            }

            col_idx += 1;
        }

        num_bytes.* = byte_pos;
    }
        


    fn readPartition(
        self: *IndexManager,
        partition_idx: usize,
        chunk_size: usize,
        final_chunk_size: usize,
        num_partitions: usize,
        num_search_cols: usize,
        search_col_idxs: []usize,
        total_docs_read: *AtomicCounter,
        progress_bar: *progress.ProgressBar,
    ) !void {
        var timer = try std.time.Timer.start();
        const interval_ns: u64 = 1_000_000_000 / 30;

        const current_chunk_size = switch (partition_idx != num_partitions - 1) {
            true => chunk_size,
            false => final_chunk_size,
        };

        const start_doc = partition_idx * chunk_size;
        const end_doc   = start_doc + current_chunk_size;

        var token_streams = try self.allocator.alloc(TokenStream, num_search_cols);

        for (0..num_search_cols) |col_idx| {
            const output_filename = try std.fmt.allocPrint(
                self.allocator, 
                "{s}/output_{d}_{d}.bin", 
                .{self.tmp_dir, partition_idx, col_idx}
                );
            defer self.allocator.free(output_filename);

            token_streams[col_idx] = try TokenStream.init(
                self.input_filename,
                output_filename,
                self.allocator,
            );
        }
        defer {
            for (0..num_search_cols) |col_idx| {
                token_streams[col_idx].deinit();
            }
            self.allocator.free(token_streams);
        }

        var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;
        var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();

        var last_doc_id: usize = 0;
        for (0.., start_doc..end_doc) |doc_id, _| {

            if (timer.read() >= interval_ns) {
                const current_docs_read = total_docs_read.fetchAdd(
                    doc_id - last_doc_id, 
                    .monotonic
                    ) + (doc_id - last_doc_id);
                last_doc_id = doc_id;
                timer.reset();

                if (partition_idx == 0) {
                    progress_bar.update(current_docs_read);
                }
            }

            var line_offset = self.index_partitions[partition_idx].line_offsets[doc_id];
            const next_line_offset = self.index_partitions[partition_idx].line_offsets[doc_id + 1];

            var search_col_idx: usize = 0;
            var prev_col: usize = 0;

            while (search_col_idx < num_search_cols) {

                for (prev_col..search_col_idxs[search_col_idx]) |_| {
                    try token_streams[0].iterField(&line_offset);
                }

                std.debug.assert(line_offset < next_line_offset);
                try self.index_partitions[partition_idx].processDocRfc4180(
                    &token_streams[search_col_idx],
                    @intCast(doc_id), 
                    &line_offset, 
                    search_col_idx,
                    &term_buffer,
                    next_line_offset,
                    &terms_seen_bitset,
                    );

                // Add one because we just iterated over the last field.
                prev_col = search_col_idxs[search_col_idx] + 1;
                search_col_idx += 1;
            }
        }

        // Flush remaining tokens.
        for (token_streams) |*stream| {
            try stream.flushTokenStream();
        }
        _ = total_docs_read.fetchAdd(end_doc - (start_doc + last_doc_id), .monotonic);

        // Construct II
        try self.index_partitions[partition_idx].constructFromTokenStream(&token_streams);
    }


    pub fn readFile(self: *IndexManager) !void {
        const file = try std.fs.cwd().openFile(self.input_filename, .{});
        defer file.close();

        const file_size = try file.getEndPos();

        const f_data = try std.posix.mmap(
            null,
            file_size,
            std.posix.PROT.READ,
            .{ .TYPE = .PRIVATE },
            file.handle,
            0
        );
        defer std.posix.munmap(f_data);

        var line_offsets = std.ArrayList(usize).init(self.allocator);
        defer line_offsets.deinit();

        var file_pos: usize = self.header_bytes;

        // Time read.
        const start_time = std.time.milliTimestamp();

        try line_offsets.append(file_pos);
        while (file_pos < file_size - 1) {

            switch (f_data[file_pos]) {
                '"' => {
                    // Iter over quote
                    file_pos += 1;

                    while (true) {

                        if (f_data[file_pos] == '"') {
                            // Escape quote. Continue to next character.
                            if (f_data[file_pos + 1] == '"') {
                                file_pos += 2;
                                continue;
                            }

                            // Iter over quote.
                            file_pos += 1;
                            break;
                        }

                        file_pos += 1;
                    }
                },
                '\n' => {
                    file_pos += 1;
                    try line_offsets.append(file_pos);
                },
                else => file_pos += 1,
            }
        }
        try line_offsets.append(file_size);

        const end_time = std.time.milliTimestamp();
        const execution_time_ms = end_time - start_time;
        const mb_s: usize = @as(usize, @intFromFloat(0.001 * @as(f32, @floatFromInt(file_size)) / @as(f32, @floatFromInt(execution_time_ms))));

        const num_lines = line_offsets.items.len - 1;

        const num_partitions = try std.Thread.getCpuCount();
        // const num_partitions = 8;

        self.file_handles = try self.allocator.alloc(std.fs.File, num_partitions);
        self.index_partitions = try self.allocator.alloc(BM25Partition, num_partitions);
        self.results_arrays = try self.allocator.alloc(sorted_array.SortedScoreArray(QueryResult), num_partitions);
        for (0..num_partitions) |idx| {
            self.file_handles[idx] = try std.fs.cwd().openFile(self.input_filename, .{});
            self.results_arrays[idx] = try sorted_array.SortedScoreArray(QueryResult).init(self.allocator, MAX_NUM_RESULTS);
        }

        std.debug.print("Writing {d} partitions\n", .{num_partitions});

        std.debug.print("Read {d} lines in {d}ms\n", .{num_lines, execution_time_ms});
        std.debug.print("{d}MB/s\n", .{mb_s});

        const chunk_size: usize = num_lines / num_partitions;
        const final_chunk_size: usize = chunk_size + (num_lines % num_partitions);

        var partition_boundaries = std.ArrayList(usize).init(self.allocator);
        defer partition_boundaries.deinit();

        const num_search_cols = self.search_cols.count();

        for (0..num_partitions) |i| {
            try partition_boundaries.append(line_offsets.items[i * chunk_size]);

            const current_chunk_size = switch (i != num_partitions - 1) {
                true => chunk_size,
                false => final_chunk_size,
            };

            const start = i * chunk_size;
            const end   = start + current_chunk_size + 1;

            const partition_line_offsets = try self.allocator.alloc(usize, current_chunk_size + 1);
            @memcpy(partition_line_offsets, line_offsets.items[start..end]);

            self.index_partitions[i] = try BM25Partition.init(
                self.allocator, 
                num_search_cols, 
                partition_line_offsets
                );
        }
        try partition_boundaries.append(file_size);

        std.debug.assert(partition_boundaries.items.len == num_partitions + 1);

        const search_col_idxs = try self.allocator.alloc(usize, num_search_cols);
        defer self.allocator.free(search_col_idxs);
        
        var map_it = self.search_cols.iterator();
        var idx: usize = 0;
        while (map_it.next()) |*item| {
            search_col_idxs[idx] = item.value_ptr.csv_idx;
            idx += 1;
        }

        const time_start = std.time.milliTimestamp();

        var threads = try self.allocator.alloc(std.Thread, num_partitions);
        defer self.allocator.free(threads);

        var total_docs_read = AtomicCounter.init(0);
        var progress_bar = progress.ProgressBar.init(num_lines);

        for (0..num_partitions) |partition_idx| {

            threads[partition_idx] = try std.Thread.spawn(
                .{},
                readPartition,
                .{
                    self,
                    partition_idx,
                    chunk_size,
                    final_chunk_size,
                    num_partitions,
                    num_search_cols,
                    search_col_idxs,
                    &total_docs_read,
                    &progress_bar,
                    },
                );
        }

        for (threads) |thread| {
            thread.join();
        }

        const _total_docs_read = total_docs_read.load(.acquire);
        std.debug.assert(_total_docs_read == line_offsets.items.len - 1);
        progress_bar.update(_total_docs_read);

        const time_end = std.time.milliTimestamp();
        const time_diff = time_end - time_start;
        std.debug.print("Processed {d} documents in {d}ms\n", .{_total_docs_read, time_diff});
    }


    pub fn queryPartition(
        self: *const IndexManager,
        queries: std.StringHashMap([]const u8),
        boost_factors: std.ArrayList(f32),
        partition_idx: usize,
        query_results: *sorted_array.SortedScoreArray(QueryResult),
    ) !void {
        const num_search_cols = self.search_cols.count();
        std.debug.assert(num_search_cols > 0);

        // Tokenize query.
        var tokens: std.ArrayList(ColTokenPair) = std.ArrayList(ColTokenPair).init(self.allocator);
        defer tokens.deinit();

        var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;

        var empty_query = true; 

        var query_it = queries.iterator();
        while (query_it.next()) |entry| {
            const _col_idx = self.search_cols.get(entry.key_ptr.*);
            if (_col_idx == null) continue;
            const col_idx = _col_idx.?.II_idx;

            var term_len: usize = 0;

            for (entry.value_ptr.*) |c| {
                if (c == ' ') {
                    if (term_len == 0) continue;

                    const token = self.index_partitions[partition_idx].II[col_idx].vocab.get(
                        term_buffer[0..term_len]
                        );
                    if (token != null) {
                        try tokens.append(ColTokenPair{
                            .col_idx = @intCast(col_idx),
                            .token = token.?,
                        });
                        empty_query = false;
                    }
                    term_len = 0;
                    continue;
                }

                term_buffer[term_len] = std.ascii.toUpper(c);
                term_len += 1;

                if (term_len == MAX_TERM_LENGTH) {
                    const token = self.index_partitions[partition_idx].II[col_idx].vocab.get(
                        term_buffer[0..term_len]
                        );
                    if (token != null) {
                        try tokens.append(ColTokenPair{
                            .col_idx = @intCast(col_idx),
                            .token = token.?,
                        });
                        empty_query = false;
                    }
                    term_len = 0;
                }
            }

            if (term_len > 0) {
                const token = self.index_partitions[partition_idx].II[col_idx].vocab.get(
                    term_buffer[0..term_len]
                    );
                if (token != null) {
                    try tokens.append(ColTokenPair{
                        .col_idx = @intCast(col_idx),
                        .token = token.?,
                    });
                    empty_query = false;
                }
            }
        }

        if (empty_query) return;


        // For each token in each II, get relevant docs and add to score.
        var doc_scores = std.AutoArrayHashMap(u32, f32).init(self.allocator);
        defer doc_scores.deinit();

        for (tokens.items) |col_token_pair| {
            const col_idx = @as(usize, @intCast(col_token_pair.col_idx));
            const token   = @as(usize, @intCast(col_token_pair.token));

            const II: *InvertedIndex = &self.index_partitions[partition_idx].II[col_idx];

            const boost_weighted_idf: f32 = (
                1.0 + std.math.log2(@as(f32, @floatFromInt(II.num_docs)) / @as(f32, @floatFromInt(II.doc_freqs.items[token])))
                ) * boost_factors.items[col_idx];

            const offset      = II.term_offsets[token];
            const last_offset = II.term_offsets[token + 1];

            for (II.postings[offset..last_offset]) |doc_token| {
                const score = boost_weighted_idf;

                const doc_id:   u32 = @intCast(doc_token.doc_id);
                // const term_pos: u32 = @intCast(doc_token.term_pos);
                std.debug.assert(doc_id < self.index_partitions[partition_idx].II[col_idx].num_docs);

                const result = try doc_scores.getOrPut(doc_id);
                if (result.found_existing) {
                    result.value_ptr.* += score;
                } else {
                    result.key_ptr.* = doc_id;
                    result.value_ptr.* = score;
                }
            }
        }

        var score_it = doc_scores.iterator();
        while (score_it.next()) |entry| {
            const score_pair = QueryResult{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.*,
                .partition_idx = partition_idx,
            };
            query_results.insert(score_pair);
        }
    }


    pub fn query(
        self: *const IndexManager,
        queries: std.StringHashMap([]const u8),
        k: usize,
        boost_factors: std.ArrayList(f32),
    ) !void {
        if (k > MAX_NUM_RESULTS) {
            std.debug.print("k must be less than or equal to {d}\n", .{MAX_NUM_RESULTS});
            return error.InvalidArgument;
        }

        // Init num_partitions threads.
        const num_partitions = self.index_partitions.len;
        var threads = try self.allocator.alloc(std.Thread, num_partitions);
        defer self.allocator.free(threads);

        // TODO: Aggregate dfs and calculate idf.

        for (0..num_partitions) |partition_idx| {
            self.results_arrays[partition_idx].clear();
            self.results_arrays[partition_idx].resize(k);
            threads[partition_idx] = try std.Thread.spawn(
                .{},
                queryPartition,
                .{
                    self,
                    queries,
                    boost_factors,
                    partition_idx,
                    &self.results_arrays[partition_idx],
                },
            );
        }

        var results = try sorted_array.SortedScoreArray(QueryResult).init(self.allocator, k);
        defer results.deinit();

        for (threads) |thread| {
            thread.join();
        }

        for (self.results_arrays) |*tr| {
            for (tr.items[0..tr.count]) |r| {
                results.insert(r);
            }
        }

        if (results.count == 0) {
            // std.debug.print("No results\n", .{});
            return;
        }
        // for (0..10) |idx| {
            // std.debug.print("Score {d}: {d} - Doc id: {d}\n", .{idx, results.items[idx].score, results.items[idx].doc_id});
        // }
        // std.debug.print("\n", .{});

        for (0..results.count) |idx| {
            const result = results.items[idx];

            try self.index_partitions[result.partition_idx].fetchRecords(
                self.result_positions[idx],
                &self.file_handles[result.partition_idx],
                result,
                @constCast(&self.result_strings[idx]),
            );
        }
    }
};


pub const QueryHandler = struct {
    index_manager: *IndexManager,
    boost_factors: std.ArrayList(f32),
    query_map: std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,
    json_objects: std.ArrayList(std.json.Value),
    output_buffer: std.ArrayList(u8),

    pub fn init(
        index_manager: *IndexManager,
        boost_factors: std.ArrayList(f32),
        query_map: std.StringHashMap([]const u8),
        allocator: std.mem.Allocator,
    ) !QueryHandler {
        return QueryHandler{
            .index_manager = index_manager,
            .boost_factors = boost_factors,
            .query_map = query_map,
            .allocator = allocator,
            .json_objects = try std.ArrayList(std.json.Value).initCapacity(allocator, MAX_NUM_RESULTS),
            .output_buffer = try std.ArrayList(u8).initCapacity(allocator, 16384),
        };
    }

    pub fn deinit(self: *QueryHandler) void {
        for (self.json_objects.items) |*json| {
            json.object.deinit();
        }
        self.json_objects.deinit();
        self.output_buffer.deinit();
    }

    fn on_request(
        self: *QueryHandler,
        r: zap.Request,
        ) !void {
        r.setHeader("Access-Control-Allow-Origin", "*") catch {};

        self.output_buffer.clearRetainingCapacity();
        self.json_objects.clearRetainingCapacity();

        const start = std.time.milliTimestamp();

        if (r.query) |query| {
            try parse_keys(
                query,
                self.query_map,
                self.index_manager.string_arena.allocator(),
            );

            // Do search.
            try self.index_manager.query(
                self.query_map,
                10,
                self.boost_factors,
                );

            for (0..10) |idx| {
                try self.json_objects.append(try csvLineToJson(
                    self.index_manager.string_arena.allocator(),
                    self.index_manager.result_strings[idx].items,
                    self.index_manager.result_positions[idx],
                    self.index_manager.cols,
                    ));
            }
            const end = std.time.milliTimestamp();
            const time_taken_ms = end - start;

            var response = std.json.Value{
                .object = std.StringArrayHashMap(std.json.Value).init(self.allocator),
            };
            defer response.object.deinit();

            try response.object.put(
                "results",
                std.json.Value{ .array = self.json_objects },
            );
            try response.object.put(
                "time_taken_ms",
                std.json.Value{ .integer = time_taken_ms },
            );

            std.json.stringify(
                response,
                .{},
                self.output_buffer.writer(),
            ) catch unreachable;

            r.sendJson(self.output_buffer.items) catch return;
        }
    }

    fn get_columns(
        self: *QueryHandler,
        r: zap.Request,
    ) !void {
        if (r.path == null) {
            std.debug.print("Request is null\n", .{});
            return;
        }

        r.setHeader("Access-Control-Allow-Origin", "*") catch |err| {
            std.debug.print("Error setting header: {?}\n", .{err});
        };

        self.output_buffer.clearRetainingCapacity();

        var response = std.json.Value{
            .object = std.StringArrayHashMap(std.json.Value).init(self.allocator),
        };

        var json_cols = try std.ArrayList(std.json.Value).initCapacity(
            self.allocator, 
            self.index_manager.cols.items.len
            );
        defer json_cols.deinit();

        for (self.index_manager.cols.items) |col| {
            try json_cols.append(std.json.Value{
                .string = col,
            });
        }

        // Swap search_cols to be first.
        var cntr: usize = 0;
        var iterator = self.index_manager.search_cols.iterator();
        while (iterator.next()) |item| {
            const csv_idx = item.value_ptr.*.csv_idx;
            const tmp = json_cols.items[csv_idx];
            json_cols.items[csv_idx] = json_cols.items[cntr];
            json_cols.items[cntr] = tmp;
            cntr += 1;
        }

        try response.object.put(
            "columns",
            std.json.Value{ .array = json_cols },
        );

        std.json.stringify(
            response,
            .{},
            self.output_buffer.writer(),
        ) catch unreachable;

        r.sendJson(self.output_buffer.items) catch return;
    }

    fn get_search_columns(
        self: *QueryHandler,
        r: zap.Request,
    ) !void {
        r.setHeader("Access-Control-Allow-Origin", "*") catch {};

        self.output_buffer.clearRetainingCapacity();

        var response = std.json.Value{
            .object = std.StringArrayHashMap(std.json.Value).init(self.allocator),
        };

        var json_cols = try std.ArrayList(std.json.Value).initCapacity(
            self.allocator, 
            self.index_manager.search_cols.count(),
            );
        defer json_cols.deinit();

        var iterator = self.index_manager.search_cols.iterator();
        while (iterator.next()) |item| {
            try json_cols.append(std.json.Value{
                .string = item.key_ptr.*,
            });
        }


        try response.object.put(
            "columns",
            std.json.Value{ .array = json_cols },
        );

        std.json.stringify(
            response,
            .{},
            self.output_buffer.writer(),
        ) catch unreachable;

        r.sendJson(self.output_buffer.items) catch return;
    }

    fn healthcheck(r: zap.Request) void {
        r.setStatus(zap.StatusCode.ok);
        r.setHeader("Access-Control-Allow-Origin", "*") catch {};
        // r.markAsFinished(true);
        r.sendBody("") catch {};
    }

    fn parse_keys(
        raw_string: []const u8,
        query_map: std.StringHashMap([]const u8),
        allocator: std.mem.Allocator,
    ) !void {
        // Format key=value&key=value
        var scratch_buffer: [4096]u8 = undefined;
        var count: usize = 0;
        var idx: usize = 0;

        while (idx < raw_string.len) {
            if (raw_string[idx] == '=') {
                idx += 1;

                const result = query_map.getPtr(scratch_buffer[0..count]);

                count = 0;
                while ((idx < raw_string.len) and (raw_string[idx] != '&')) {
                    if (raw_string[idx] == '+') {
                        scratch_buffer[count] = ' ';
                        count += 1;
                        idx += 1;
                        continue;
                    }
                    scratch_buffer[count] = std.ascii.toUpper(raw_string[idx]);
                    count += 1;
                    idx   += 1;
                }
                if (result != null) {
                    const value_copy = try allocator.dupe(u8, scratch_buffer[0..count]);
                    result.?.* = value_copy;
                }
                count = 0;
                idx += 1;
                continue;
            }
            scratch_buffer[count] = std.ascii.toUpper(raw_string[idx]);
            count += 1;
            idx   += 1;
        }
    }
};

fn bench() !void {
    // const filename: []const u8 = "../tests/mb_small.csv";
    const filename: []const u8 = "../tests/mb.csv";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var search_cols = std.ArrayList([]u8).init(allocator);
    const title:  []u8 = try allocator.dupe(u8, "Title");
    const artist: []u8 = try allocator.dupe(u8, "ARTIST");
    const album:  []u8 = try allocator.dupe(u8, "ALBUM");

    try search_cols.append(title);
    try search_cols.append(artist);
    try search_cols.append(album);

    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    // index_manager.printDebugInfo();

    defer {
        allocator.free(title);
        allocator.free(artist);
        allocator.free(album);

        search_cols.deinit();
        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }

    var query_map = std.StringHashMap([]const u8).init(allocator);
    defer query_map.deinit();

    try query_map.put("TITLE", "JOHNSON");
    try query_map.put("ARTIST", "BILLY BOYA");
    try query_map.put("ALBUM", "LIGHTNING");

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(2.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    try index_manager.query(
        query_map,
        10,
        boost_factors,
        );

    const num_queries: usize = 1_000;

    const start_time = std.time.milliTimestamp();
    for (0..num_queries) |_| {
        try index_manager.query(
            query_map,
            10,
            boost_factors,
            );
    }
    const end_time = std.time.milliTimestamp();
    const execution_time_ms = (end_time - start_time);
    const qps = @as(f64, @floatFromInt(num_queries)) / @as(f64, @floatFromInt(execution_time_ms)) * 1000;

    std.debug.print("\n\n================================================\n", .{});
    std.debug.print("QUERIES PER SECOND: {d}\n", .{qps});
    std.debug.print("================================================\n", .{});
}

fn test_main() !void {
    const API = false;

    const filename: []const u8 = "../tests/mb_small.csv";
    // const filename: []const u8 = "../tests/mb.csv";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var search_cols = std.ArrayList([]u8).init(allocator);
    const title:  []u8 = try allocator.dupe(u8, "Title");
    const artist: []u8 = try allocator.dupe(u8, "ARTIST");
    const album:  []u8 = try allocator.dupe(u8, "ALBUM");

    try search_cols.append(title);
    try search_cols.append(artist);
    try search_cols.append(album);

    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    // index_manager.printDebugInfo();

    defer {
        allocator.free(title);
        allocator.free(artist);
        allocator.free(album);

        search_cols.deinit();
        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }

    var query_map = std.StringHashMap([]const u8).init(allocator);
    defer query_map.deinit();

    try query_map.put("TITLE", "FRANK SINATRA");
    try query_map.put("ARTIST", "FRANK SINATRA");
    try query_map.put("ALBUM", "FRANK SINATRA");

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(2.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    try index_manager.query(
        query_map,
        10,
        boost_factors,
        );

    for (index_manager.result_positions[0]) |pos| {
        const start_byte = pos.start_pos;
        const end_byte   = start_byte + pos.field_len;
        std.debug.print("{s},", .{index_manager.result_strings[0].items[start_byte..end_byte]});
    }

    if (API) {
        var query_handler = try QueryHandler.init(
            &index_manager,
            boost_factors,
            query_map,
            allocator,
        );
        query_handler.deinit();

        var simple_router = zap.Router.init(allocator, .{});

        try simple_router.handle_func("/search", &query_handler, &QueryHandler.on_request);
        try simple_router.handle_func("/get_columns", &query_handler, &QueryHandler.get_columns);
        try simple_router.handle_func("/get_search_columns", &query_handler, &QueryHandler.get_search_columns);
        try simple_router.handle_func("/healthcheck", &query_handler, &QueryHandler.healthcheck);

        defer simple_router.deinit();
        var listener = zap.HttpListener.init(.{
            .port = 5000,
            .on_request = simple_router.on_request_handler(),
            .log = true,
        });
        try listener.listen();

    
        std.debug.print("\n\n\nListening on 0.0.0.0:5000\n", .{});

        // start worker threads
        zap.start(.{
            .threads = 1,
            .workers = 1,
        });
    }
}

fn main_cli_runner() !void {
    // Embed files
    const index_html = @embedFile("index.html");
    const style_css = @embedFile("style.css");
    const table_js = @embedFile("table.js");

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    var filename: []const u8 = undefined;

    var search_cols = std.ArrayList([]u8).init(allocator);

    var idx: usize = 0;
    while (args.next()) |arg| {
        if (idx == 0) {
            // Skip binary name.
        } else if (idx == 1) {
            filename = arg;
        } else {
            try search_cols.append(try allocator.dupe(u8, arg));
        }
        idx += 1;
    }
    if (idx < 3) {
        @panic("Add filename - Usage: ./bm25 <filename> <search_col1> <search_col2> ...");
    }


    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    // Write files to disk
    var output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/index.html", 
        .{index_manager.tmp_dir}
        );
    var output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(index_html);

    output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/style.css",
        .{index_manager.tmp_dir}
        );
    output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(style_css);

    output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/table.js",
        .{index_manager.tmp_dir}
        );
    output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(table_js);

    // index_manager.printDebugInfo();

    var query_map = std.StringHashMap([]const u8).init(allocator);
    var boost_factors = std.ArrayList(f32).init(allocator);

    for (search_cols.items) |col| {
        try query_map.put(col, "test");
        try boost_factors.append(1.0);
    }

    var query_handler = try QueryHandler.init(
        &index_manager,
        boost_factors,
        query_map,
        allocator,
    );


    var simple_router = zap.Router.init(
        allocator, 
        .{},
        );

    try simple_router.handle_func("/search", &query_handler, &QueryHandler.on_request);
    try simple_router.handle_func("/get_columns", &query_handler, &QueryHandler.get_columns);
    try simple_router.handle_func("/get_search_columns", &query_handler, &QueryHandler.get_search_columns);
    try simple_router.handle_func("/healthcheck", &query_handler, &QueryHandler.healthcheck);

    var listener = zap.HttpListener.init(.{
        .port = 5000,
        .on_request = simple_router.on_request_handler(),
        .log = true,
    });
    try listener.listen();

    const full_path = try std.mem.concat(
        index_manager.string_arena.allocator(),
        u8, 
        &[_][]const u8{index_manager.tmp_dir, "/index.html"}
        );

    var cmd = std.process.Child.init(&[_][]const u8{"open", full_path}, allocator);
    try cmd.spawn();
    _ = try cmd.wait();

    defer {
        for (search_cols.items) |col| {
            allocator.free(col);
        }

        search_cols.deinit();
        query_map.deinit();
        boost_factors.deinit();

        query_handler.deinit();
        simple_router.deinit();

        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }


    std.debug.print("\n\n\nListening on 0.0.0.0:5000\n", .{});

    // start worker threads
    zap.start(.{
        .threads = 1,
        .workers = 1,
    });
}

pub fn main() !void {
    // try test_main();
    // try main_cli_runner();
    try bench();
}
