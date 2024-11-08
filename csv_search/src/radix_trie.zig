const std = @import("std");

const MAX_STRING_LEN = 7;


pub fn LCP(key: []const u8, match: []const u8) u8 {
    var len: usize = 0;
    for (key) |c| {
        if (c != match[len]) return @intCast(len);
        len += 1;
    }
    return len;
}

const RadixEdge = extern struct {
    edge_str: [MAX_STRING_LEN]u8,
    edge_len: u8,
    child_ptr: *RadixNode,

    pub fn init(allocator: std.mem.Allocator) !*RadixEdge {
        const edge = allocator.create(RadixEdge);
        edge.* = RadixEdge{
            .edge_str = undefined,
            .edge_len = 0,
            .child_ptr = undefined,
        };
        return edge;
    }

    pub inline fn getStr(self: *const RadixEdge) *[]u8 {
        return self.edge_str[0..self.edge_len];
    }

};

const RadixNode = packed struct {
    num_edges: u8,
    is_leaf: bool,
    _pad: u16,
    value: u32,
    edges: [*]RadixEdge,

    pub fn init(allocator: std.mem.Allocator) !*RadixNode {
        const node = try allocator.create(RadixNode);
        node.* = RadixNode{
            .num_edges = 0,
            .is_leaf = undefined,
            ._pad = undefined,
            .value = undefined,
            .edges = undefined,
        };
        return node;
    }
};

const RadixTrie = struct {
    arena: *std.heap.ArenaAllocator,
    root: *RadixNode,
    num_nodes: u32,


    pub fn init(arena: *std.heap.ArenaAllocator) !RadixTrie {
        const root = try RadixNode.init(arena.allocator());
        return RadixTrie{
            .arena = arena,
            .root = root,
            .num_nodes = 0,
        };
    }

    pub fn insert(self: *RadixTrie, key: []const u8, value: u32) !void {
        var node = self.root;

        while (true) {
            var next_node: *RadixNode = undefined;
            var max_lcp: u8 = 0;
            for (0..node.num_edges) |edge_idx| {
                const current_prefix = node.edges[edge_idx].getStr();
                const lcp = LCP(key, current_prefix);

                if (lcp > max_lcp) {
                    max_lcp = lcp;
                    next_node = node.edges[edge_idx].child_ptr;
                }
            }


            if (max_lcp == 0) {
                // TODO: Check for exceeding MAX_STRING_LEN

                // Create new node
                const new_node = try RadixNode.init(self.arena.allocator());
                new_node.value = value;
                new_node.is_leaf = true;

                node.is_leaf = false;

                // Create new edge
                node.edges[node.num_edges] = try RadixEdge.init(self.arena.allocator());
                node.edges[node.num_edges].* = RadixEdge{
                    .edge_str = key,
                    .edge_len = key.len,
                    .child_ptr = new_node,
                };
                node.num_edges += 1;

                return;
            }

            // node = next_node;
        }
    }
};



















test "bench" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var keys = std.StringHashMap(usize).init(allocator);
    defer keys.deinit();

    var trie = try RadixTrie.init(&arena);

    const _N: usize = 1;
    const num_chars: usize = 6;
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

    const init_bytes: usize = arena.queryCapacity();

    const start = std.time.milliTimestamp();
    var it = keys.iterator();
    var i: u32 = 0;
    while (it.next()) |entry| {
        try trie.insert(entry.key_ptr.*, i);
        i += 1;
    }
    const end = std.time.milliTimestamp();
    const elapsed = end - start;

    const big_N: u64 = N * @as(u64, 1000);
    const insertions_per_second = @as(f32, @floatFromInt(big_N)) / @as(f32, @floatFromInt(elapsed));
    std.debug.print("\nConstruction time: {}ms\n", .{elapsed});
    std.debug.print("Insertions per second: {d}\n", .{insertions_per_second});

    // Get total bytes allocated
    const total_bytes: usize = trie.mem_usage;
    std.debug.print("Hashmap MB allocated: {d}MB\n", .{(init_bytes) / (1024 * 1024)});
    std.debug.print("Trie MB allocated:    {d}MB\n", .{total_bytes / (1024 * 1024)});

    try std.testing.expectEqual(N, trie.terminal_count);
}
