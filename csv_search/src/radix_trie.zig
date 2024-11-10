const std = @import("std");

const print = std.debug.print;

const MAX_STRING_LEN = 7;


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

    pub fn printChildren(self: *const RadixNode, depth: u32) void {
        for (0..self.num_edges) |edge_idx| {
            const edge = self.edges[edge_idx];
            print("Depth {d} - Edge: {s}\n",  .{depth, edge.str[0..edge.len]});
            print("Depth {d} - Child: {d}\n\n", .{depth, edge.child_ptr.value});
            edge.child_ptr.printChildren(depth + 1);
        }
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
        var node = _node;
        var rem_chars = key.len;

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
                key[0..num_chars_edge],
                );

            try node.addEdge(self.allocator, new_edge);

            if (is_leaf) break;

            rem_chars -= MAX_STRING_LEN;
            node = new_node;
        }
    }

    pub fn insert(self: *RadixTrie, key: []const u8, value: u32) !void {
        var node = self.root;
        var key_idx: usize = 0;

        // TODO: Break at MAX_STRING_LEN chars.
        while (true) {
            var next_node: *RadixNode = undefined;
            var max_edge_idx: usize = 0;
            var max_lcp: u8 = 0;
            var partial: bool = false;

            for (0..node.num_edges) |edge_idx| {
                const current_edge   = node.edges[edge_idx];
                const current_prefix = current_edge.str[0..current_edge.len];
                const lcp = LCP(key[key_idx..], current_prefix);

                if (lcp > max_lcp) {
                    max_lcp   = lcp;
                    max_edge_idx = edge_idx;
                    next_node = node.edges[edge_idx].child_ptr;
                    partial   = lcp < current_prefix.len;
                }
            }
            const rem_chars: usize = key.len - key_idx - max_lcp;

            if (max_lcp == 0) {
                try self.addNode(key[key_idx..], node, value);
                self.num_keys += 1;
                return;
            }

            // Matched rest of key. Node already exists. Error for now. 
            // Make behavior user defined later.
            if (!partial and (max_lcp == key[key_idx..].len)) return error.AlreadyExistsError;

            key_idx += max_lcp;
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

                    try new_node.addEdge(self.allocator, new_edge);
                    self.num_keys += 1;
                } else {
                    // TODO: Modify for case where rem_chars > MAX_STRING_LEN.

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

                    try new_node_1.addEdge(self.allocator, new_edge_2);

                    // New node addition handler here, to account for rem_chars > MAX_STRING_LEN.
                    try self.addNode(key[key.len-rem_chars..], new_node_1, value);
                }

                self.num_keys += 1;
                return;
            }

            // Traverse
            node = next_node;
        }
    }

    pub fn find(self: *RadixTrie, key: []const u8) u32 {
        var node = self.root;
        var key_idx: usize = 0;
        var depth: usize = 0;

        while (true) {
            var matched = false;

            for (0..node.num_edges) |edge_idx| {
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

            if (!matched) return std.math.maxInt(u32);
            depth += 1;
            
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

    try trie.insert("test", 420);
    try trie.insert("testing", 69);
    try trie.insert("tes", 24);
    try trie.insert("waddup", 54);
    try trie.insert("newting", 54);
    try trie.insert("tosting", 69);
    try trie.insert("abracadabra", 121);

    trie.printNodes();

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
    const num_chars: usize = 7;
    const rand = std.crypto.random;
    for (0.._N) |i| {
        var key = try allocator.alloc(u8, num_chars);
        for (0..num_chars) |j| {
            key[j] = rand.int(u8) % 12 + @as(u8, 'a');
        }
        try keys.put(key, i);
    }

    const N = keys.count();
    std.debug.print("N: {d}\n", .{N});

    var start = std.time.milliTimestamp();
    var it = keys.iterator();
    var i: u32 = 0;
    while (it.next()) |entry| {
        try trie.insert(entry.key_ptr.*, i);
        i += 1;
    }
    var end = std.time.milliTimestamp();
    const elapsed_insert = end - start;

    start = std.time.milliTimestamp();
    it = keys.iterator();
    while (it.next()) |entry| {
        _ = trie.find(entry.key_ptr.*);
    }
    end = std.time.milliTimestamp();
    const elapsed_find = end - start;

    const big_N: u64 = N * @as(u64, 1000);
    const insertions_per_second = @as(f32, @floatFromInt(big_N)) / @as(f32, @floatFromInt(elapsed_insert));
    const lookups_per_second    = @as(f32, @floatFromInt(big_N)) / @as(f32, @floatFromInt(elapsed_find));
    std.debug.print("\nConstruction time:   {}ms\n", .{elapsed_insert});
    std.debug.print("Insertions per second: {d}\n", .{insertions_per_second});
    std.debug.print("Lookups per second:    {d}\n", .{lookups_per_second});

    try std.testing.expectEqual(N, trie.num_keys);
}
