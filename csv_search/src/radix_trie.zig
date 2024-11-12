const std = @import("std");

const print = std.debug.print;

const MAX_STRING_LEN = 7;

// Most frequent characters in English.
const CHAR_FREQ_TABLE: [256]u8 = blk: {
    var table: [256]u8 = undefined;
    for (0..256) |idx| {
        table[idx] = 0;
    }

    table['e'] = 1;
    table['a'] = 2;
    table['r'] = 3;
    table['i'] = 4;
    table['o'] = 5;
    table['t'] = 6;
    table['n'] = 7;

    break :blk table;
};

const FULL_MASKS: [8]u8 = [_]u8{
    0b11111110,
    0b11111100,
    0b11111000,
    0b11110000,
    0b11100000,
    0b11000000,
    0b10000000,
    0b00000000,
};
const BIT_MASKS: [8]u8 = [_]u8{
    0b00000001,
    0b00000010,
    0b00000100,
    0b00001000,
    0b00010000,
    0b00100000,
    0b01000000,
    0b10000000,
};


pub inline fn getInsertIdx(bitmask: u8, char: u8) usize {
    const shift_len: usize = @intCast(CHAR_FREQ_TABLE[char]);
    const num_bits_populated_front = @popCount(bitmask & FULL_MASKS[shift_len]);
    return num_bits_populated_front;
}

pub fn LCP(key: []const u8, match: []const u8) u8 {
    const max_chars = @min(key.len, match.len);
    for (0..max_chars) |idx| {
        if (key[idx] != match[idx]) return @intCast(idx);
    }
    return @intCast(max_chars);
}

const RadixEdge = extern struct {
    str: [MAX_STRING_LEN]u8,
    len: u8,
    child_ptr: *RadixNode,

    pub fn init(allocator: std.mem.Allocator) !*RadixEdge {
        const edge = try allocator.create(RadixEdge);
        edge.* = RadixEdge{
            .str = undefined,
            .len = 0,
            .child_ptr = undefined,
        };
        return edge;
    }
};

const RadixNode = packed struct {
    num_edges: u8,
    edgelist_capacity: u8,
    is_leaf: bool,
    freq_char_bitmask: u8,
    value: u32,
    edges: [*]RadixEdge,

    pub fn init(allocator: std.mem.Allocator) !*RadixNode {
        const node = try allocator.create(RadixNode);
        node.* = RadixNode{
            .num_edges = 0,
            .edgelist_capacity = 0,
            .is_leaf = false,
            .freq_char_bitmask = 0,
            .value = undefined,
            .edges = undefined,
        };
        return node;
    }

    pub inline fn addEdge(
        self: *RadixNode, 
        allocator: std.mem.Allocator,
        edge: *RadixEdge,
        ) !void {
        self.num_edges += 1;

        if (self.num_edges > self.edgelist_capacity) {
            const new_capacity: usize = @min(256, (2 * self.edgelist_capacity) + 1);
            const old_slice = self.edges[0..self.edgelist_capacity];

            const new_slice: []RadixEdge = try allocator.realloc(
                    old_slice,
                    new_capacity,
                    );
            self.edges = new_slice.ptr;
            self.edgelist_capacity = @intCast(new_capacity);
        }
        self.edges[self.num_edges - 1] = edge.*;
    }

    pub inline fn addEdgePos(
        self: *RadixNode, 
        allocator: std.mem.Allocator,
        edge: *RadixEdge,
        ) !void {
        if (CHAR_FREQ_TABLE[edge.str[0]] == 0) {
            try self.addEdge(allocator, edge);
            return;
        }

        const insert_idx_new: usize = getInsertIdx(self.freq_char_bitmask, edge.str[0]);
        const old_swap_idx: usize   = @intCast(self.num_edges);

        try self.addEdge(allocator, edge);
        if (insert_idx_new == old_swap_idx) return;

        // Save for swap. Will be overwritten in memmove.
        const temp = self.edges[old_swap_idx];

        // Memmove
        var idx: usize = @intCast(self.num_edges - 1);
        while (idx > insert_idx_new) : (idx -= 1) {
            self.edges[idx] = self.edges[idx - 1];
        }

        // Swap
        self.edges[insert_idx_new] = temp;
    }

    pub fn printChildren(self: *const RadixNode, depth: u32) void {
        for (0..self.num_edges) |edge_idx| {
            const edge = self.edges[edge_idx];
            print("Depth {d} - Edge: {s}\n",  .{depth, edge.str[0..edge.len]});
            print("Depth {d} - Child: {d}\n\n", .{depth, edge.child_ptr.value});
            edge.child_ptr.printChildren(depth + 1);
        }
    }

    pub fn printEdges(self: *const RadixNode) void {
        for (0..self.num_edges) |i| {
            const edge = self.edges[i];
            print("{d}: {s}\n", .{i, edge.str[0..edge.len]});
        }
        print("\n", .{});
    }
};

const RadixTrie = struct {
    allocator: std.mem.Allocator,
    root: *RadixNode,
    num_keys: u32,


    pub fn init(allocator: std.mem.Allocator) !RadixTrie {
        const root = try RadixNode.init(allocator);
        return RadixTrie{
            .allocator = allocator,
            .root = root,
            .num_keys = 0,
        };
    }

    inline fn addNode(
        self: *RadixTrie, 
        key: []const u8, 
        _node: *RadixNode,
        value: u32,
    ) !void {
        var node      = _node;
        var rem_chars = key.len;

        var current_idx: usize = 0;
        while (true) {
            var num_chars_edge = rem_chars;
            var is_leaf = true;

            if (rem_chars > MAX_STRING_LEN) {
                num_chars_edge = MAX_STRING_LEN;
                is_leaf = false;
            }

            const new_node   = try RadixNode.init(self.allocator);
            new_node.value   = value;
            new_node.is_leaf = is_leaf;

            const new_edge     = try RadixEdge.init(self.allocator);
            new_edge.len       = @truncate(num_chars_edge);
            new_edge.child_ptr = new_node;

            @memcpy(
                new_edge.str[0..num_chars_edge], 
                key[current_idx..current_idx+num_chars_edge],
                );
            node.freq_char_bitmask |= (BIT_MASKS[CHAR_FREQ_TABLE[new_edge.str[0]]] & 0b11111110);

            if (CHAR_FREQ_TABLE[new_edge.str[0]] == 0) {
                try node.addEdge(self.allocator, new_edge);
            } else {
                try node.addEdgePos(self.allocator, new_edge);
            }

            if (is_leaf) break;

            rem_chars   -= MAX_STRING_LEN;
            current_idx += num_chars_edge;
            node         = new_node;
        }
    }

    pub fn insert(self: *RadixTrie, key: []const u8, value: u32) !void {
        var node = self.root;
        var key_idx: usize = 0;

        while (true) {
            var next_node: *RadixNode = undefined;
            var max_edge_idx: usize = 0;
            var max_lcp: u8 = 0;
            var partial: bool = false;

            const shift_len: usize = @intCast(CHAR_FREQ_TABLE[key[key_idx]]);

            if (shift_len > 0) {
                if ((BIT_MASKS[shift_len] & node.freq_char_bitmask) == 0) {

                    // Node doesn't exist. Insert.
                    try self.addNode(key[key_idx..], node, value);
                    self.num_keys += 1;
                    return;
                }

                const access_idx: usize = getInsertIdx(node.freq_char_bitmask, key[key_idx]);

                const edge   = node.edges[access_idx];
                max_lcp      = LCP(key[key_idx..], edge.str[0..edge.len]);
                next_node    = node.edges[access_idx].child_ptr;
                partial      = max_lcp < edge.len;
                max_edge_idx = access_idx;
            } else {
                const start_idx = @popCount(node.freq_char_bitmask);
                for (start_idx..node.num_edges) |edge_idx| {
                    const current_edge   = node.edges[edge_idx];
                    const current_prefix = current_edge.str[0..current_edge.len];
                    const lcp = LCP(key[key_idx..], current_prefix);

                    if (lcp > max_lcp) {
                        max_lcp   = lcp;
                        max_edge_idx = edge_idx;
                        next_node = node.edges[edge_idx].child_ptr;
                        partial   = lcp < current_prefix.len;
                        break;
                    }
                }
                if (max_lcp == 0) {
                    try self.addNode(key[key_idx..], node, value);
                    self.num_keys += 1;
                    return;
                }
            }

            key_idx += max_lcp;

            // Matched rest of key. Node already exists. Error for now. 
            // Make behavior user defined later.
            // if (!partial and (max_lcp == key[key_idx..].len)) return error.AlreadyExistsError;
            if (!partial and (key_idx == key.len)) return error.AlreadyExistsError;

            const rem_chars: usize = key.len - key_idx;
            if (partial) {
                // Split
                if (rem_chars == 0) {
                    var existing_edge = &node.edges[max_edge_idx];
                    const existing_node = existing_edge.child_ptr;
                    const new_node = try RadixNode.init(self.allocator);
                    const new_edge = try RadixEdge.init(self.allocator);

                    /////////////////////////////////
                    //                             //
                    // \ - existing_edge           //
                    //  O - existing_node          //
                    //                             //
                    // str:           'ABC'        //
                    // existing_edge: 'ABCD'       //
                    //                             //
                    // --------------------------- //
                    //      SPLIT NO-CREATE        //
                    // --------------------------- //
                    // \ - existing_edge           // 
                    //  O - new_node               //
                    //   \ - new_edge              //
                    //    O - existing_node        //
                    //                             //
                    // existing_edge: 'ABC'        //
                    // new_edge:      'D'          //
                    //                             //
                    /////////////////////////////////
                    new_node.is_leaf = true;

                    new_edge.len      = existing_edge.len - max_lcp;
                    existing_edge.len = max_lcp;

                    @memcpy(
                        new_edge.str[0..new_edge.len], 
                        existing_edge.str[0..new_edge.len],
                        );

                    existing_edge.child_ptr = new_node;
                    new_edge.child_ptr      = existing_node;

                    new_node.value = value;
                    new_node.freq_char_bitmask = (BIT_MASKS[CHAR_FREQ_TABLE[new_edge.str[0]]] & 0b11111110);

                    try new_node.addEdgePos(self.allocator, new_edge);
                    self.num_keys += 1;
                } else {
                    var existing_edge = &node.edges[max_edge_idx];
                    const existing_node = existing_edge.child_ptr;
                    const new_node_1 = try RadixNode.init(self.allocator);
                    const new_edge_2 = try RadixEdge.init(self.allocator);

                    /////////////////////////////////////////
                    //                                     //
                    // \ - existing_edge                   //
                    //  O - existing_node                  //
                    //                                     //
                    // str:           'AEF'                //
                    // existing_edge: 'ABCD'               //
                    //                                     //
                    // ----------------------------------- //
                    //             SPLIT CREATE            //
                    // ----------------------------------- //
                    //  \ - existing_edge                  // 
                    //   O - new_node_1                    //
                    //  / \ - (new_edge_1, new_edge_2)     //
                    // O   O - (new_node_2, existing_node) //
                    //                                     //
                    // existing_edge: 'A'                  //
                    // new_edge_1:    'BCD'                //
                    // new_edge_2:    'EF'                 //
                    //                                     //
                    /////////////////////////////////////////

                    new_edge_2.len    = existing_edge.len - max_lcp;
                    existing_edge.len = max_lcp;

                    @memcpy(
                        new_edge_2.str[0..new_edge_2.len], 
                        existing_edge.str[max_lcp..max_lcp + new_edge_2.len],
                        );

                    new_edge_2.child_ptr    = existing_node;
                    existing_edge.child_ptr = new_node_1;

                    try new_node_1.addEdgePos(self.allocator, new_edge_2);

                    try self.addNode(key[key.len-rem_chars..], new_node_1, value);
                }

                self.num_keys += 1;
                return;
            }

            // Traverse
            node = next_node;
        }
    }

    pub fn find(self: *const RadixTrie, key: []const u8) u32 {
        var node = self.root;
        var key_idx: usize = 0;

        while (true) {
            var matched = false;
            if (key_idx >= key.len) return std.math.maxInt(u32);


            const shift_len: usize = @intCast(CHAR_FREQ_TABLE[key[key_idx]]);
            if (shift_len > 0) {
                const access_idx = @popCount(node.freq_char_bitmask & FULL_MASKS[shift_len]);
                const current_edge   = node.edges[access_idx];
                const current_prefix = current_edge.str[0..current_edge.len];

                if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                    matched  = true;
                    node     = current_edge.child_ptr;
                    key_idx += current_prefix.len;

                    if ((key_idx == key.len) and node.is_leaf) return node.value;
                }
            } else {
                const start_idx = @popCount(node.freq_char_bitmask);
                for (start_idx..node.num_edges) |edge_idx| {
                    const current_edge   = node.edges[edge_idx];
                    const current_prefix = current_edge.str[0..current_edge.len];

                    if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                        matched  = true;
                        node     = node.edges[edge_idx].child_ptr;
                        key_idx += current_prefix.len;

                        if ((key_idx == key.len) and node.is_leaf) return node.value;
                        break;
                    }
                }
            }

            if (!matched) return std.math.maxInt(u32);
        }
    }

    pub fn printNodes(self: *const RadixTrie) void {
        self.root.printChildren(0);
        print("\n\n", .{});
    }
};


test "insertion" {
    print("\n\n", .{});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var trie = try RadixTrie.init(allocator);

    try trie.insert("ostritch", 25);
    try trie.insert("test", 420);
    try trie.insert("testing", 69);
    try trie.insert("tes", 24);
    try trie.insert("waddup", 54);
    try trie.insert("newting", 44);
    try trie.insert("tosting", 69);
    try trie.insert("abracadabra", 121);
    try trie.insert("toaster", 84);
    try trie.insert("rapper", 85);
    try trie.insert("apple", 25);
    try trie.insert("apocryphol", 25);
    try trie.insert("apacryphol", 25);
    try trie.insert("apicryphol", 25);
    try trie.insert("eager", 25);
    try trie.insert("mantequilla", 25);
    try trie.insert("initial", 25);

    // trie.printNodes();
    // trie.root.printEdges();

    try std.testing.expectEqual(420, trie.find("test"));
    try std.testing.expectEqual(69, trie.find("testing"));
    try std.testing.expectEqual(24, trie.find("tes"));
    try std.testing.expectEqual(420, trie.find("test"));
    try std.testing.expectEqual(69, trie.find("testing"));
    try std.testing.expectEqual(54, trie.find("waddup"));
    try std.testing.expectEqual(std.math.maxInt(u32), trie.find("testin"));
    try std.testing.expectEqual(121, trie.find("abracadabra"));
}

test "bench" {
    // @breakpoint();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer {
        // _ = gpa.deinit();
    // }
    // const g_allocator = gpa.allocator();

    var keys = std.StringHashMap(usize).init(allocator);
    defer keys.deinit();

    var trie = try RadixTrie.init(allocator);

    const _N: usize = 1_000_000;
    const num_chars: usize = 6;
    const rand = std.crypto.random;

    const raw_keys = try allocator.alloc([num_chars]u8, _N);
    for (0.._N) |i| {
        for (0..num_chars) |j| {
            raw_keys[i][j] = rand.int(u8) % 26 + @as(u8, 'a');
        }
    }

    var start = std.time.milliTimestamp();
    for (0.._N) |i| {
        try keys.put(&raw_keys[i], i);
    }
    var end = std.time.milliTimestamp();
    const elapsed_insert_hashmap = end - start;

    const N = keys.count();
    std.debug.print("N: {d}\n", .{N});

    start = std.time.milliTimestamp();
    var it = keys.iterator();
    var i: u32 = 0;
    while (it.next()) |entry| {
        try trie.insert(entry.key_ptr.*, i);
        i += 1;
    }
    end = std.time.milliTimestamp();
    const elapsed_insert_trie = end - start;

    start = std.time.milliTimestamp();
    it = keys.iterator();
    while (it.next()) |entry| {
        _ = trie.find(entry.key_ptr.*);
    }
    end = std.time.milliTimestamp();
    const elapsed_trie_find = end - start;

    start = std.time.milliTimestamp();
    it = keys.iterator();
    while (it.next()) |entry| {
        _ = keys.get(entry.key_ptr.*);
    }
    end = std.time.milliTimestamp();
    const elapsed_hashmap_find = end - start;

    const big_N: u64 = N * @as(u64, 1000);
    const insertions_per_second_trie = @as(f32, @floatFromInt(big_N)) / @as(f32, @floatFromInt(elapsed_insert_trie));
    const lookups_per_second_trie    = @as(f32, @floatFromInt(big_N)) / @as(f32, @floatFromInt(elapsed_trie_find));
    std.debug.print("-------------------  RADIX TRIE  -------------------\n", .{});
    std.debug.print("\nConstruction time:   {}ms\n", .{elapsed_insert_trie});
    std.debug.print("Insertions per second: {d}\n", .{insertions_per_second_trie});
    std.debug.print("Lookups per second:    {d}\n", .{lookups_per_second_trie});

    std.debug.print("\n\n", .{});

    const insertions_per_second_hashmap = @as(f32, @floatFromInt(big_N)) / @as(f32, @floatFromInt(elapsed_insert_hashmap));
    const lookups_per_second_hashmap    = @as(f32, @floatFromInt(big_N)) / @as(f32, @floatFromInt(elapsed_hashmap_find));
    std.debug.print("-------------------  HASHMAP  -------------------\n", .{});
    std.debug.print("\nConstruction time:   {}ms\n", .{elapsed_insert_hashmap});
    std.debug.print("Insertions per second: {d}\n", .{insertions_per_second_hashmap});
    std.debug.print("Lookups per second:    {d}\n", .{lookups_per_second_hashmap});


    try std.testing.expectEqual(N, trie.num_keys);
}
