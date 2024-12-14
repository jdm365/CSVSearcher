const std = @import("std");
const progress = @import("progress.zig");
const sorted_array = @import("sorted_array.zig");
const ScorePair = @import("sorted_array.zig");
const builtin = @import("builtin");
const TermPos = @import("server.zig").TermPos;
const csvLineToJson = @import("server.zig").csvLineToJson;
const csvLineToJsonScore = @import("server.zig").csvLineToJsonScore;
const zap = @import("zap");

const print = std.debug.print;
const assert = std.debug.assert;

const TOKEN_STREAM_CAPACITY = 1_048_576;
const MAX_LINE_LENGTH       = 1_048_576;
const MAX_NUM_TERMS         = 4096;
const MAX_TERM_LENGTH       = 64;
const MAX_NUM_RESULTS       = 1000;


inline fn sumBool(slice: []bool) usize {
    var sum: usize = 0;
    for (slice) |val| {
        sum += @intFromBool(val);
    }
    return sum;
}
const AtomicCounter = std.atomic.Value(u64);

const ScoringInfo = packed struct {
    score: f32,
    term_pos: u8,
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

const ColTokenPair = packed struct {
    col_idx: u24,
    term_pos: u8,
    token: u32,
};

const score_f32 = struct {
    score: f32,
};

pub fn TopKPQ(
    comptime T: type,
    comptime Context: type,
    comptime compareFn: fn (context: Context, a: T, b: T) std.math.Order,
    ) type {

    return struct{
        const Self = @This();

        pq: std.PriorityQueue(T, Context, compareFn),
        k: usize,

        pub fn init(
            allocator: std.mem.Allocator, 
            context: Context,
            k: usize,
            ) Self {
            const pq = std.PriorityQueue(T, context, compareFn).init(allocator);
            return Self{
                .pq = pq,
                .k = k,
            };
        }

        pub fn deinit(self: *Self) Self {
            self.pq.deinit();
        }

        pub fn add(self: *Self, entry: T) !void {
            try self.pq.add(entry);
            if (self.pq.items.len > self.k) {
                self.pq.removeIndex(self.k);
            }
        }
    };
}

pub fn BitVector(comptime num_bits: usize) type {
    comptime {
        assert(@popCount(num_bits) == 1);
    }

    return struct {
        const Self = @This();

        data: [@divFloor(num_bits, 8)]u8,

        pub inline fn add(self: *Self, idx: usize) void {
            self.data[@divFloor(idx, 8) + @mod(idx, 8)] |= 1;
        }

        pub inline fn clear(self: *Self) void {
            @memset(self.data, 0);
        }

        pub inline fn hamming(self: *const Self, other: *const Self) usize {
            var _hamming: usize = 0;
            inline for (0..@divFloor(num_bits, 8)) |byte_idx| {
                _hamming += @popCount(self.data[byte_idx] & other.data[byte_idx]);
            }
            return _hamming;
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
            .start_pos = @as(u32, @intCast(start_pos)) + @intFromBool(buffer[start_pos] == '"'),
            .field_len = @as(u32, @intCast(byte_pos - start_pos - 1)) - @intFromBool(buffer[start_pos] == '"'),
        };
    }
}

const CSVStream = struct {
    f_data: []align(std.mem.page_size) u8,

    pub fn init(filename: []const u8) !CSVStream {

        const file = try std.fs.cwd().openFile(filename, .{});
        const file_size = try file.getEndPos();

        const csv_stream = CSVStream{
            .f_data = try std.posix.mmap(
                null,
                file_size,
                std.posix.PROT.READ,
                .{ .TYPE = .PRIVATE },
                file.handle,
                0
            ),
        };

        return csv_stream;
    }

    pub fn deinit(self: *CSVStream) void {
        std.posix.munmap(self.f_data);
    }
    
    pub fn iterField(self: *CSVStream, byte_pos: *usize) !void {
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


// ENTROPY ASSIGNMENTS ON A PER RECORD BASIS.
// LOW ENTROPY RECORDS (OR SMALL DON'T KNOW YET) GET SMALLER BITVECTORS.

const BitVectorIndex = struct {
    vectors: []BitVector(1024),
    num_docs: u32,

    pub fn init(
        allocator: std.mem.Allocator,
        num_docs: usize,
        ) !BitVectorIndex {

        var vectors = allocator.alloc(BitVector(1024), num_docs);
        for (0..num_docs) |idx| {
            const unroll_size = @min(4, num_docs - idx);
            inline for (idx..idx+unroll_size) |jdx| {
                vectors[jdx].clear();
            }
        }

        const II = BitVectorIndex{
            .vectors = vectors,
            .num_docs = @intCast(num_docs),
        };
        @memset(II.doc_sizes, 0);
        return II;
    }

    pub fn deinit(
        self: *BitVectorIndex,
        allocator: std.mem.Allocator,
        ) void {
        allocator.free(self.vectors);
    }
};

const BitVectorPartition = struct {
    II: []BitVectorIndex,
    hash_seeds: [8]u64,
    line_offsets: []usize,
    allocator: std.mem.Allocator,
    string_arena: std.heap.ArenaAllocator,
    doc_score_map: std.AutoHashMap(u32, ScoringInfo),

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        line_offsets: []usize,
    ) !BitVectorPartition {
        var doc_score_map = std.AutoHashMap(u32, ScoringInfo).init(allocator);
        try doc_score_map.ensureTotalCapacity(50_000);

        const partition = BitVectorPartition{
            .II = try allocator.alloc(BitVectorIndex, num_search_cols),
            .hash_seeds = &[_]usize{0, 1, 2, 3, 4, 5, 6, 7},
            .line_offsets = line_offsets,
            .allocator = allocator,
            .string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .doc_score_map = doc_score_map,
        };

        for (0..num_search_cols) |idx| {
            partition.II[idx] = try BitVectorIndex.init(allocator, line_offsets.len - 1);
        }

        return partition;
    }

    pub fn deinit(self: *BitVectorPartition) void {
        self.allocator.free(self.line_offsets);
        for (0..self.II.len) |i| {
            self.II[i].deinit(self.allocator);
        }
        self.allocator.free(self.II);
        self.string_arena.deinit();
        self.doc_score_map.deinit();
    }

    inline fn hash(
        self: *const BitVectorPartition, 
        term: []u8,
        target_bitvector: *BitVector(1024),
        ) void {
        inline for (0..8) |idx| {
            const insert_idx = @mod(std.hash.Wyhash.hash(self.hash_seeds[idx], term), 128) + 128 * idx;
            target_bitvector.add(@as(usize, @intCast(insert_idx)));
        }
    }

    fn addToken(
        self: *BitVectorPartition,
        term: *[MAX_TERM_LENGTH]u8,
        cntr: *usize,
        doc_id: u32,
        col_idx: usize,
        new_doc: *bool,
        byte_idx: *usize,
    ) !void {
        if (cntr.* == 0) {
            byte_idx.* += 1;
            return;
        }

        self.hash(term[0..cntr.*], &self.II[col_idx].vectors[doc_id]);

        cntr.* = 0;
        byte_idx.* += 1;
        new_doc.* = false;
    }


    pub fn processDocRfc4180(
        self: *BitVectorPartition,
        token_stream: *CSVStream,
        doc_id: u32,
        byte_idx: *usize,
        col_idx: usize,
        term: *[MAX_TERM_LENGTH]u8,
        max_byte: usize,
    ) !void {
        const start_byte = byte_idx.*;

        const is_quoted  = (token_stream.f_data[byte_idx.*] == '"');
        byte_idx.* += @intFromBool(is_quoted);

        var cntr: usize = 0;
        var new_doc: bool = (doc_id != 0);

        if (is_quoted) {

            while (true) {
                if (self.II[col_idx].doc_sizes[doc_id] >= MAX_NUM_TERMS) {
                    byte_idx.* = start_byte;
                    try token_stream.iterField(byte_idx);
                    return;
                }
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

                switch (token_stream.f_data[byte_idx.*]) {
                    ' ' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '.' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '-' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '/' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '+' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '=' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '&' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    else => {
                        if (cntr == MAX_TERM_LENGTH - 1) {
                            self.hash(term[0..cntr], &self.II[col_idx].vectors[doc_id]);

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

        } else {

            while (true) {
                std.debug.assert(byte_idx.* < max_byte);

                switch (token_stream.f_data[byte_idx.*]) {
                    ',' => break,
                    '\n' => break,
                    ' ' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '.' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '-' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '/' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '+' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '=' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    '&' => try self.addToken(
                        term, 
                        &cntr, 
                        doc_id, 
                        col_idx, 
                        &new_doc,
                        byte_idx,
                        ),
                    else => {
                        if (cntr == MAX_TERM_LENGTH - 1) {
                            self.hash(term[0..cntr], &self.II[col_idx].vectors[doc_id]);

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
            self.hash(term[0..cntr], &self.II[col_idx].vectors[doc_id]);
        }

        byte_idx.* += 1;
    }


    // pub fn constructFromCSVStream(
        // self: *BitVectorPartition,
        // token_streams: *[]CSVStream,
        // ) !void {

        // for (0.., self.II) |col_idx, *II| {
            // var term_offsets = try self.allocator.alloc(usize, II.num_terms);
            // defer self.allocator.free(term_offsets);
            // @memset(term_offsets, 0);

            // // Create index.
            // const ts = token_streams.*[col_idx];
            // try ts.output_file.seekTo(0);

            // var bytes_read: usize = 0;

            // var num_tokens: usize = TOKEN_STREAM_CAPACITY;
            // var current_doc_id: usize = 0;

            // while (num_tokens == TOKEN_STREAM_CAPACITY) {
                // var _num_tokens: [4]u8 = undefined;
                // _ = try ts.output_file.read(std.mem.asBytes(&_num_tokens));
                // const endianness = builtin.cpu.arch.endian();
                // num_tokens = std.mem.readInt(u32, &_num_tokens, endianness);

                // bytes_read = try ts.output_file.read(
                    // std.mem.sliceAsBytes(ts.tokens[0..num_tokens])
                    // );
                // std.debug.assert(bytes_read == 4 * num_tokens);

                // var token_count: usize = 0;
                // for (token_count..token_count + num_tokens) |idx| {
                    // if (@as(*u32, @ptrCast(&ts.tokens[idx])).* == std.math.maxInt(u32)) {
                        // // Null token.
                        // current_doc_id += 1;
                        // continue;
                    // }

                    // const new_doc  = ts.tokens[idx].new_doc;
                    // const term_pos = ts.tokens[idx].term_pos;
                    // const term_id: usize = @intCast(ts.tokens[idx].doc_id);

// 
                    // current_doc_id += @intCast(new_doc);

                    // const token = token_t{
                        // .new_doc = 0,
                        // .term_pos = term_pos,
                        // .doc_id = @truncate(current_doc_id),
                    // };

                    // const postings_offset = II.term_offsets[term_id] + term_offsets[term_id];
                    // std.debug.assert(postings_offset < II.postings.len);
                    // std.debug.assert(current_doc_id < II.num_docs);

                    // term_offsets[term_id] += 1;

                    // II.postings[postings_offset] = token;
                // }

                // token_count += num_tokens;
            // }
        // }
    // }

    pub fn fetchRecords(
        self: *const BitVectorPartition,
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
    index_partitions: []BitVectorPartition,
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
            self.results_arrays[idx].deinit();
            self.index_partitions[idx].deinit();
            self.file_handles[idx].close();
        }
        self.allocator.free(self.file_handles);
        self.allocator.free(self.index_partitions);
        self.allocator.free(self.results_arrays);

        self.search_cols.deinit();
        self.cols.deinit();

        try std.fs.cwd().deleteTree(self.tmp_dir);
        self.allocator.free(self.tmp_dir);

        for (0..MAX_NUM_RESULTS) |idx| {
            self.allocator.free(self.result_positions[idx]);
            self.result_strings[idx].deinit();
        }

        self.string_arena.deinit();
    }

    fn printDebugInfo(self: *const IndexManager) void {
        std.debug.print("\n=====================================================\n", .{});

        var num_terms: usize = 0;
        var num_docs: usize = 0;
        var avg_doc_size: f32 = 0.0;

        for (0..self.index_partitions.len) |idx| {

            num_docs += self.index_partitions[idx].II[0].num_docs;
            for (0..self.index_partitions[idx].II.len) |jdx| {
                num_terms    += self.index_partitions[idx].II[jdx].num_terms;
                avg_doc_size += self.index_partitions[idx].II[jdx].avg_doc_size;
            }

        }

        std.debug.print("Num Partitions:  {d}\n", .{self.index_partitions.len});

        var col_iterator = self.search_cols.iterator();
        while (col_iterator.next()) |item| {
            std.debug.print("Column:          {s}\n", .{item.key_ptr.*});
            std.debug.print("---------------------------------------------\n", .{});
            std.debug.print("Num terms:       {d}\n", .{num_terms});
            std.debug.print("Num docs:        {d}\n", .{num_docs});
            std.debug.print("Avg doc size:    {d}\n\n", .{avg_doc_size / @as(f32, @floatFromInt(self.index_partitions.len))});
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

        var token_streams = try self.allocator.alloc(CSVStream, num_search_cols);

        for (0..num_search_cols) |col_idx| {
            const output_filename = try std.fmt.allocPrint(
                self.allocator, 
                "{s}/output_{d}_{d}.bin", 
                .{self.tmp_dir, partition_idx, col_idx}
                );
            defer self.allocator.free(output_filename);

            token_streams[col_idx] = try CSVStream.init(
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

        // Sort search_col_idxs
        std.sort.insertion(usize, search_col_idxs, {}, comptime std.sort.asc(usize));

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
                    );

                // Add one because we just iterated over the last field.
                prev_col = search_col_idxs[search_col_idx] + 1;
                search_col_idx += 1;
            }
        }

        // Flush remaining tokens.
        // for (token_streams) |*stream| {
            // try stream.flushCSVStream();
        // }
        _ = total_docs_read.fetchAdd(end_doc - (start_doc + last_doc_id), .monotonic);

        // Construct II
        try self.index_partitions[partition_idx].constructFromCSVStream(&token_streams);
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

        // const num_partitions = try std.Thread.getCpuCount();
        const num_partitions = 1;

        self.file_handles = try self.allocator.alloc(std.fs.File, num_partitions);
        self.index_partitions = try self.allocator.alloc(BitVectorPartition, num_partitions);
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

            self.index_partitions[i] = try BitVectorPartition.init(
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

    pub fn queryPartitionOrdered(
        self: *const IndexManager,
        queries: std.StringHashMap([]const u8),
        boost_factors: std.ArrayList(f32),
        partition_idx: usize,
        query_results: *sorted_array.SortedScoreArray(QueryResult),
    ) !void {
        const num_search_cols = self.search_cols.count();
        std.debug.assert(num_search_cols > 0);

        var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;

        var query_bitvectors = self.allocator.alloc(BitVector(1024), num_search_cols);
        defer self.allocator.free(query_bitvectors);

        var search_cols_mask = self.allocator.alloc(bool, num_search_cols);
        defer self.allocator.free(search_cols_mask);

        var query_it = queries.iterator();
        while (query_it.next()) |entry| {
            const _col_idx = self.search_cols.get(entry.key_ptr.*);
            if (_col_idx == null) continue;
            const col_idx = _col_idx.?.II_idx;

            var term_len: usize = 0;

            for (entry.value_ptr.*) |c| {
                if (c == ' ') {
                    if (term_len == 0) continue;

                    self.index_partitions[partition_idx].II.hash(
                        term_buffer[0..term_len],
                        query_bitvectors[col_idx],
                    );
                    term_len = 0;
                    search_cols_mask[col_idx] = true;
                    continue;
                }

                term_buffer[term_len] = std.ascii.toUpper(c);
                term_len += 1;

                if (term_len == MAX_TERM_LENGTH) {
                    self.index_partitions[partition_idx].II.hash(
                        term_buffer[0..term_len],
                        query_bitvectors[col_idx],
                    );
                    search_cols_mask[col_idx] = true;
                    term_len = 0;
                }
            }

            if (term_len > 0) {
                self.index_partitions[partition_idx].II.hash(
                    term_buffer[0..term_len],
                    query_bitvectors[col_idx],
                );
                search_cols_mask[col_idx] = true;
                term_len = 0;
            }
        }

        if (sumBool(search_cols_mask) == 0) return;

        // For each token in each II, get relevant docs and add to score.
        var doc_scores: *std.AutoHashMap(u32, ScoringInfo) = &self.index_partitions[partition_idx].doc_score_map;
        doc_scores.clearRetainingCapacity();

        var sorted_scores = try sorted_array.SortedScoreArray(score_f32).init(
            self.allocator, 
            query_results.capacity,
            );
        defer sorted_scores.deinit();


        var done = false;

        const pq = TopKPQ(usize, void, std.math.order).init(self.allocator).init(
            self.allocator,
            void,
            query_results.capacity,
        );
        defer pq.deinit();

        for (0..num_search_cols) |col_idx| {
            if (!search_cols_mask[col_idx]) continue;

            const query_vector  = &query_bitvectors[col_idx];
            const index_vectors = &self.index_partitions[partition_idx].II[col_idx].vectors;

            var _hammings: [4]usize = undefined; 
            for (0..index_vectors.len) |idx| {
                const unroll_size = @min(4, index_vectors.len - idx);
                inline for (0..unroll_size) |jdx| {
                    _hammings[idx] = query_vector.hamming(index_vectors[idx+jdx]);
                }
                for (0..unroll_size) |jdx| {
                    pq.add(_hammings[jdx]);
                }
            }
        }

        var last_col_idx: usize = 0;
        for (0..tokens.items.len) |idx| {
            const score = token_scores[idx];
            const col_score_pair = tokens.items[idx];

            const col_idx = @as(usize, @intCast(col_score_pair.col_idx));
            const token   = @as(usize, @intCast(col_score_pair.token));

            const II: *BitVectorIndex = &self.index_partitions[partition_idx].II[col_idx];

            const offset      = II.term_offsets[token];
            const last_offset = II.term_offsets[token + 1];

            const is_high_df_term: bool = (score < IDF_THRESHOLD) or
                                          (score < 0.4 * idf_sum / @as(f32, @floatFromInt(tokens.items.len)));

            var prev_doc_id: u32 = std.math.maxInt(u32);
            for (II.postings[offset..last_offset]) |doc_token| {
                const doc_id:   u32 = @intCast(doc_token.doc_id);
                const term_pos: u8  = @intCast(doc_token.term_pos);

                prev_doc_id = doc_id;

                const _result = doc_scores.getPtr(doc_id);
                if (_result) |result| {
                    // TODO: Consider front of record boost.

                    // Phrase boost.
                    const last_term_pos = result.*.term_pos;
                    result.*.score += @as(f32, @floatFromInt(@intFromBool((term_pos == last_term_pos + 1) and (col_idx == last_col_idx) and (doc_id == prev_doc_id)))) * score * 0.75;

                    // Does tf scoring effectively.
                    result.*.score += score;

                    result.*.term_pos = term_pos;

                    const score_copy = result.*.score;
                    sorted_scores.insert(score_f32{
                        .score = score_copy,
                    });

                } else {
                    if (!done and !is_high_df_term) {

                        if (sorted_scores.count == sorted_scores.capacity - 1) {
                            const min_score = sorted_scores.items[sorted_scores.count - 1];
                            if (min_score.score > idf_remaining) {
                                done = true;
                                continue;
                            }
                        }

                        try doc_scores.put(
                            doc_id,
                            ScoringInfo{
                                .score = score,
                                .term_pos = term_pos,
                            }
                        );
                        sorted_scores.insert(score_f32{
                            .score = score,
                        });
                    }
                }
            }

            // std.debug.print("TOTAL TERMS SCORED: {d}\n", .{doc_scores.count()});
            // std.debug.print("WAS HIGH DF TERM:   {}\n\n", .{is_high_df_term});
            idf_remaining -= score;
            last_col_idx = col_idx;
        }

        // std.debug.print("\nTOTAL TERMS SCORED: {d}\n", .{doc_scores.count()});

        var score_it = doc_scores.iterator();
        while (score_it.next()) |entry| {

            const score_pair = QueryResult{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.*.score,
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

        for (0..num_partitions) |partition_idx| {
            self.results_arrays[partition_idx].clear();
            self.results_arrays[partition_idx].resize(k);
            threads[partition_idx] = try std.Thread.spawn(
                .{},
                queryPartitionOrdered,
                .{
                    self,
                    queries,
                    boost_factors,
                    partition_idx,
                    &self.results_arrays[partition_idx],
                },
            );
        }

        for (threads) |thread| {
            thread.join();
        }

        if (self.index_partitions.len > 1) {
            for (self.results_arrays[1..]) |*tr| {
                for (tr.items[0..tr.count]) |r| {
                    self.results_arrays[0].insert(r);
                }
            }
        }
        if (self.results_arrays[0].count == 0) return;

        for (0..self.results_arrays[0].count) |idx| {
            const result = self.results_arrays[0].items[idx];

            try self.index_partitions[result.partition_idx].fetchRecords(
                self.result_positions[idx],
                &self.file_handles[result.partition_idx],
                result,
                @constCast(&self.result_strings[idx]),
            );
            // std.debug.print("Score {d}: {d} - Doc id: {d}\n", .{idx, self.results_arrays[0].items[idx].score, self.results_arrays[0].items[idx].doc_id});
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

    fn on_request(self: *QueryHandler, r: zap.Request) !void {
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
                try self.json_objects.append(try csvLineToJsonScore(
                    self.index_manager.string_arena.allocator(),
                    self.index_manager.result_strings[idx].items,
                    self.index_manager.result_positions[idx],
                    self.index_manager.cols,
                    self.index_manager.results_arrays[0].items[idx].score,
                    idx,
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

    fn get_columns(self: *QueryHandler, r: zap.Request) !void {
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
        try json_cols.append(std.json.Value{
            .string = "SCORE",
        });
        const csv_idx = json_cols.items.len - 1;
        const tmp = json_cols.items[csv_idx];
        json_cols.items[csv_idx] = json_cols.items[cntr];
        json_cols.items[cntr] = tmp;
        

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


test "bench" {
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

    index_manager.printDebugInfo();

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

    try query_map.put("TITLE", "UNDER MY SKIN");
    try query_map.put("ARTIST", "FRANK SINATRA");
    try query_map.put("ALBUM", "LIGHTNING");

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(1.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    const num_queries: usize = 5_000;

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
