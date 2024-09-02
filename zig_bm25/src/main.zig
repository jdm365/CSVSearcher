const std = @import("std");
const builtin = @import("builtin");

const TOKEN_STREAM_CAPACITY = 1_048_576;
const MAX_NUM_TERMS = 4096;
const MAX_TERM_LENGTH = 64;

const AtomicCounter = std.atomic.Value(u64);
const token_t = packed struct(u32) {
    term_pos: u8,
    doc_id: u24
};

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
        term_pos: u8,
        doc_id: u32,
    ) !void {
        self.tokens[self.num_terms] = token_t{
            .term_pos = term_pos,
            .doc_id = @intCast(doc_id),
        };
        self.num_terms += 1;

        if (self.num_terms == TOKEN_STREAM_CAPACITY) {
            try self.flushTokenStream();
        }
    }

    pub fn flushTokenStream(self: *TokenStream) !void {
        const bytes_to_write = @sizeOf(token_t) * self.num_terms;
        const bytes_written = try self.output_file.write(
            std.mem.sliceAsBytes(self.tokens[0..self.num_terms])
            );
        
        if (bytes_written != bytes_to_write) {
            return error.WriteError;
        }

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
    vocab: std.StringArrayHashMap(u32),
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
        const II = InvertedIndex{
            .postings = &[_]token_t{},
            .vocab = std.StringArrayHashMap(u32).init(allocator),
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
        var iter = self.vocab.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.vocab.deinit();

        allocator.free(self.term_offsets);
        self.doc_freqs.deinit();
        allocator.free(self.doc_sizes);
    }

    pub fn resizePostings(
        self: *InvertedIndex,
        allocator: std.mem.Allocator,
        ) !void {
        self.num_terms = self.doc_freqs.items.len;
        self.term_offsets = try allocator.alloc(u32, self.num_terms);

        // Num terms is now known.
        var postings_size: usize = 0;
        for (0.., self.doc_freqs) |i, doc_freq| {
            self.term_offsets[i] = postings_size;
            postings_size += doc_freq;
        }
        self.term_offsets[self.num_terms - 1] = postings_size;
        self.postings = try allocator.alloc(token_t, postings_size);

        var avg_doc_size: f64 = 0.0;
        for (self.doc_sizes) |doc_size| {
            avg_doc_size += doc_size;
        }
        avg_doc_size /= self.num_docs;
        self.avg_doc_size = @as(f32, avg_doc_size);
    }
};

const BM25Partition = struct {
    II: []InvertedIndex,
    line_offsets: []usize,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        num_cols: usize,
        line_offsets: []usize,
    ) !BM25Partition {
        const partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndex, num_cols),
            .line_offsets = line_offsets,
            .allocator = allocator,
        };

        for (0..num_cols) |i| {
            partition.II[i] = try InvertedIndex.init(allocator, line_offsets.len);
        }

        return partition;
    }

    pub fn deinit(self: *BM25Partition) void {
        self.allocator.free(self.line_offsets);
        for (0..self.II.len) |i| {
            self.II[i].deinit(self.allocator);
        }
        self.allocator.free(self.II);
    }
};



fn addTerm(
    term: []u8,
    term_len: usize,
    doc_id: u32,
    term_pos: u8,
    index_partition: *BM25Partition,
    col_idx: usize,
    token_stream: *TokenStream,
    terms_seen: *std.bit_set.StaticBitSet(MAX_NUM_TERMS),
    allocator: std.mem.Allocator,
) !void {
    const gop = try index_partition.II[col_idx].vocab.getOrPut(term[0..term_len]);
    if (!gop.found_existing) {
        const term_copy = try allocator.dupe(u8, term[0..term_len]);
        errdefer allocator.free(term_copy);

        gop.key_ptr.* = term_copy;
        gop.value_ptr.* = index_partition.II[col_idx].num_terms;
        index_partition.II[col_idx].num_terms += 1;
        try index_partition.II[col_idx].doc_freqs.append(1);
    } else {
        if (!terms_seen.isSet(gop.value_ptr.* % MAX_NUM_TERMS)) {
            index_partition.II[col_idx].doc_freqs.items[gop.value_ptr.*] += 1;
        }
    }

    index_partition.II[col_idx].doc_sizes[doc_id] += 1;

    try token_stream.addToken(term_pos, doc_id);
}

pub fn processDocRfc4180(
    token_stream: *TokenStream,
    index_partition: *BM25Partition,
    doc_id: u32,
    byte_idx: *usize,
    col_idx: usize,
    term: *[MAX_TERM_LENGTH]u8,
    max_byte: usize,
) !void {
    var term_pos: u8 = 0;
    const is_quoted  = (token_stream.f_data[byte_idx.*] == '"');
    byte_idx.* += @intFromBool(is_quoted);

    var cntr: usize = 0;

    var terms_seen: std.bit_set.StaticBitSet(MAX_NUM_TERMS) = undefined;

    if (is_quoted) {

        while (true) {
            std.debug.assert(index_partition.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);
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

                try addTerm(
                    term, 
                    cntr, 
                    doc_id, 
                    term_pos, 
                    index_partition, 
                    col_idx, 
                    token_stream, 
                    &terms_seen,
                    index_partition.allocator,
                    );

                if (term_pos != 255) {
                    term_pos += 1;
                }
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
            std.debug.assert(index_partition.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);
            std.debug.assert(byte_idx.* < max_byte);

            if ((token_stream.f_data[byte_idx.*] == ',') or (token_stream.f_data[byte_idx.*] == '\n')) {
                break;
            }

            if ((token_stream.f_data[byte_idx.*] == ' ') or (cntr == MAX_TERM_LENGTH - 1)) {
                if (cntr == 0) {
                    byte_idx.* += 1;
                    continue;
                }

                try addTerm(
                    term, 
                    cntr, 
                    doc_id, 
                    term_pos, 
                    index_partition, 
                    col_idx, 
                    token_stream, 
                    &terms_seen,
                    index_partition.allocator,
                    );

                cntr = 0;
                byte_idx.* += 1;

                if (term_pos != 255) {
                    term_pos += 1;
                }
                continue;
            }

            term[cntr] = std.ascii.toUpper(token_stream.f_data[byte_idx.*]);
            cntr += 1;
            byte_idx.* += 1;
        }
    }

    if (cntr > 0) {
        std.debug.assert(index_partition.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

        try addTerm(
            term, 
            cntr, 
            doc_id, 
            term_pos, 
            index_partition, 
            col_idx, 
            token_stream, 
            &terms_seen,
            index_partition.allocator,
            );
    }

    byte_idx.* += 1;
}


const IndexManager = struct {
    index_partitions: []BM25Partition,
    input_filename: []const u8,
    allocator: std.mem.Allocator,
    search_cols: std.StringArrayHashMap(u16),
    tmp_dir: []const u8,

    fn readCSVHeader(
        input_filename: []const u8,
        search_cols: *std.ArrayList([]const u8),
        allocator: std.mem.Allocator,
        ) !std.StringArrayHashMap(u16) {
        var col_map = std.StringArrayHashMap(u16).init(allocator);
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
                            for (0..search_cols.items.len) |i| {
                                if (std.mem.eql(u8, term[0..cntr], search_cols.items[i])) {
                                    try col_map.put(term[0..cntr], @intCast(col_idx));
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
                            for (0..search_cols.items.len) |i| {
                                if (std.mem.eql(u8, term[0..cntr], search_cols.items[i])) {
                                    try col_map.put(term[0..cntr], @intCast(col_idx));
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
    }
        

    pub fn init(
        input_filename: []const u8,
        search_cols: *std.ArrayList([]const u8),
        allocator: std.mem.Allocator,
        ) !IndexManager {

        const search_col_map = try readCSVHeader(input_filename, search_cols, allocator);
        const num_partitions = try std.Thread.getCpuCount();

        std.debug.print("Writing {d} partitions\n", .{num_partitions});

        const file_hash = blk: {
            var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(input_filename, &hash, .{});
            break :blk hash;
        };
        const dir_name = try std.fmt.allocPrint(
            allocator,
            ".{x:0>32}", .{std.fmt.fmtSliceHexLower(file_hash[0..16])}
            );

        try std.fs.cwd().makeDir(dir_name);

        return IndexManager{
            .index_partitions = try allocator.alloc(BM25Partition, num_partitions),
            .input_filename = input_filename,
            .allocator = allocator,
            .search_cols = search_col_map,
            .tmp_dir = dir_name,
        };
    }
    
    pub fn deinit(self: *IndexManager) !void {
        for (0..self.index_partitions.len) |i| {
            self.index_partitions[i].deinit();
        }
        self.search_cols.deinit();
        self.allocator.free(self.index_partitions);

        try std.fs.cwd().deleteTree(self.tmp_dir);
        self.allocator.free(self.tmp_dir);
    }


    fn readPartition(
        self: *IndexManager,
        partition_idx: usize,
        chunk_size: usize,
        final_chunk_size: usize,
        num_partitions: usize,
        line_offsets: *const std.ArrayList(usize),
        partition_boundaries: *const std.ArrayList(usize),
        num_cols: usize,
        search_col_idxs: []u16,
        total_docs_read: *AtomicCounter,
    ) !void {
        var timer = try std.time.Timer.start();
        const interval_ns: u64 = 1_000_000_000 / 15;

        var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;

        const current_chunk_size = switch (partition_idx != num_partitions - 1) {
            true => chunk_size,
            false => final_chunk_size,
        };

        const end_pos   = partition_boundaries.items[partition_idx + 1];
        const start_doc = partition_idx * chunk_size;
        const end_doc   = start_doc + current_chunk_size;

        const output_filename = try std.fmt.allocPrint(
            self.allocator, 
            "{s}/output_{d}.bin", 
            .{self.tmp_dir, partition_idx}
            );
        defer self.allocator.free(output_filename);

        var token_stream = try TokenStream.init(
            self.input_filename, 
            output_filename,
            self.allocator
            );
        defer token_stream.deinit();

        var last_doc_id: usize = 0;
        for (0.., start_doc..end_doc) |doc_id, _| {

            if (timer.read() >= interval_ns) {
                const current_docs_read = total_docs_read.fetchAdd(doc_id - last_doc_id, .monotonic) + (doc_id - last_doc_id);
                last_doc_id = doc_id;
                timer.reset();

                if (partition_idx == 0) {
                    std.debug.print("Read {d}/{d} docs\r", .{current_docs_read, line_offsets.items.len});
                }
            }

            var line_offset = line_offsets.items[doc_id];

            const next_line_offset = switch (doc_id == line_offsets.items.len - 1) {
                true => end_pos,
                false => line_offsets.items[doc_id + 1],
            };

            var search_col_idx: usize = 0;
            var prev_col: usize = 0;

            while (search_col_idx < num_cols) {

                for (prev_col..search_col_idxs[search_col_idx]) |_| {
                    try token_stream.iterField(&line_offset);
                }

                std.debug.assert(line_offset < next_line_offset);
                try processDocRfc4180(
                    &token_stream, 
                    &self.index_partitions[partition_idx],
                    @intCast(doc_id), 
                    &line_offset, 
                    search_col_idx,
                    &term_buffer,
                    next_line_offset,
                    );
                prev_col = search_col_idxs[search_col_idx];
                search_col_idx += 1;
            }
        }

        // Flush remaining tokens.
        try token_stream.flushTokenStream();
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

        var file_pos: usize = 0;

        // Time read.
        const start_time = std.time.milliTimestamp();

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

        const end_time = std.time.milliTimestamp();
        const execution_time_ms = end_time - start_time;
        const mb_s: usize = @as(usize, @intFromFloat(0.001 * @as(f32, @floatFromInt(file_size)) / @as(f32, @floatFromInt(execution_time_ms))));

        std.debug.print("Read {d} lines in {d}ms\n", .{line_offsets.items.len, execution_time_ms});
        std.debug.print("{d}MB/s\n", .{mb_s});

        const num_lines = line_offsets.items.len;
        const num_partitions = self.index_partitions.len;
        const chunk_size: usize = num_lines / num_partitions;
        const final_chunk_size: usize = chunk_size + (num_lines % num_partitions);

        var partition_boundaries = std.ArrayList(usize).init(self.allocator);
        defer partition_boundaries.deinit();

        const num_cols = self.search_cols.count();

        for (0..num_partitions) |i| {
            try partition_boundaries.append(line_offsets.items[i * chunk_size]);

            const current_chunk_size = switch (i != num_partitions - 1) {
                true => chunk_size,
                false => final_chunk_size,
            };

            const start = i * chunk_size;
            const end = start + current_chunk_size;

            const partition_line_offsets = try self.allocator.alloc(usize, current_chunk_size);
            @memcpy(partition_line_offsets, line_offsets.items[start..end]);

            self.index_partitions[i] = try BM25Partition.init(
                self.allocator, 
                num_cols, 
                partition_line_offsets
                );
        }
        try partition_boundaries.append(file_size);

        std.debug.assert(partition_boundaries.items.len == num_partitions + 1);

        const search_col_idxs = self.search_cols.values();
        const time_start = std.time.milliTimestamp();

        var threads = try self.allocator.alloc(std.Thread, num_partitions);
        defer self.allocator.free(threads);

        var total_docs_read = AtomicCounter.init(0);

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
                    &line_offsets,
                    &partition_boundaries,
                    num_cols,
                    search_col_idxs,
                    &total_docs_read
                    },
                );
        }

        for (threads) |thread| {
            thread.join();
        }

        const _total_docs_read = total_docs_read.load(.acquire);
        std.debug.assert(_total_docs_read == line_offsets.items.len);
        std.debug.print("Read {d}/{d} docs\r", .{_total_docs_read, line_offsets.items.len});

        const time_end = std.time.milliTimestamp();
        const time_diff = time_end - time_start;
        std.debug.print("Processed {d} documents in {d}ms\n", .{line_offsets.items.len, time_diff});
    }

};


pub fn main() !void {
    const filename: []const u8 = "../tests/mb.csv";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = blk: {
        if (builtin.mode == .ReleaseFast) {
            break :blk std.heap.c_allocator;
        }
        break :blk gpa.allocator();
    }; 

    var search_cols = std.ArrayList([]const u8).init(allocator);
    try search_cols.append("TITLE");
    try search_cols.append("ARTIST");

    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    defer {
        search_cols.deinit();
        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }
}
