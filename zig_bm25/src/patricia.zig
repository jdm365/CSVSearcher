const std = @import("std");

fn commonPrefixLen(a: []const u8, b: []const u8) usize {
    const len = @min(a.len, b.len);
    var i: usize = 0;
    while (i < len and a[i] == b[i]) : (i += 1) {}
    return i;
}

pub const bitfield_u32 = packed struct (u32) {
    terminal: bool,
    value: u31,
};

const Node = struct {
    prefix: []const u8,
    value: bitfield_u32,
    children: std.ArrayList(Node),

    fn init(allocator: std.mem.Allocator) !Node {
        return Node{
            .prefix = &[_]u8{},
            .value = bitfield_u32{
                .terminal = false,
                .value = 0,
            },
            .children = std.ArrayList(Node).init(allocator),
        };
    }

    fn deinit(self: Node, allocator: std.mem.Allocator) void {
        if (self.prefix.len > 0) {
            allocator.free(self.prefix);
        }
        self.children.deinit();
    }
};

const PatriciaTrie = struct {
    root: Node,
    allocator: std.mem.Allocator,
    terminal_count: usize,

    pub fn init(allocator: std.mem.Allocator) !PatriciaTrie {
        const root = try Node.init(allocator);
        return PatriciaTrie{
            .root = root,
            .allocator = allocator,
            .terminal_count = 0,
        };
    }

    pub fn deinit(self: *PatriciaTrie) void {
        self.root.deinit(self.allocator);
    }

    pub fn insert(self: *PatriciaTrie, key: []const u8, value: u32) !void {
        var current = &self.root;
        var i: usize = 0;

        while (i < key.len) {
            var longest_prefix: usize = 0;
            var matching_child: *Node = undefined;

            for (current.children.items) |*child| {

                std.debug.assert(child.children.items.len <= 256);

                const common_len = commonPrefixLen(child.prefix, key[i..]);
                if (common_len > longest_prefix) {
                    longest_prefix = common_len;
                    matching_child = child;
                }
            }

            if (longest_prefix == 0) {

                // No matching prefix, create a new node
                var new_node    = try Node.init(self.allocator);
                new_node.prefix = try self.allocator.dupe(u8, key[i..]);
                new_node.value.terminal = true;
                new_node.value.value = @intCast(value);
                try current.children.append(new_node);
                self.terminal_count += 1;

                std.debug.assert(new_node.children.items.len <= 256);
                return;
            }

            if (longest_prefix < matching_child.prefix.len) {
                // The key is a prefix of the existing node
                // Need to split the existing node

                // Create new node for remaining suffix.
                // Will be child of `matching_child` and contain remaining suffix.
                var new_node    = try Node.init(self.allocator);
                // new_node.prefix = try self.allocator.dupe(u8, matching_child.prefix[longest_prefix..]);
                new_node.prefix = matching_child.prefix[longest_prefix..];
                std.debug.assert(new_node.children.items.len <= 256);

                for (matching_child.children.items) |child| {
                    try new_node.children.append(child);
                }
                new_node.value = matching_child.value;

                try matching_child.children.append(new_node);
                self.terminal_count += 1;
                matching_child.prefix = matching_child.prefix[0..longest_prefix];
                matching_child.value.terminal = false;

                std.debug.assert(matching_child.children.items.len <= 256);

                if (i + longest_prefix < key.len) {
                    // Still more unmatched prefix remaining.
                    // Create new terminal node with remainder of key.
                    var key_node = try Node.init(self.allocator);
                    key_node.prefix = try self.allocator.dupe(u8, key[i + longest_prefix..]);
                    key_node.value.terminal = true;
                    key_node.value.value = @intCast(value);
                    try matching_child.children.append(key_node);

                    std.debug.assert(key_node.children.items.len <= 256);
                } else if (i + longest_prefix == key.len) {
                    matching_child.value.terminal = true;
                    matching_child.value.value = @intCast(value);

                    std.debug.assert(new_node.children.items.len <= 256);
                }

                return;
            }

            current = matching_child;
            i += longest_prefix;
        }
    }

    pub fn search(self: *const PatriciaTrie, key: []const u8) bool {
        var current = &self.root;
        var i: usize = 0;

        while (i < key.len) {
            var longest_prefix: usize = 0;
            var matching_child: *Node = undefined;

            for (current.children.items) |*child| {
                const common_len = commonPrefixLen(child.prefix, key[i..]);
                if (common_len > longest_prefix) {
                    longest_prefix = common_len;
                    matching_child = child;
                }
            }

            if (longest_prefix == 0) {
                return false;
            }

            if (i + matching_child.prefix.len > key.len) {
                // The search key is a prefix of this node's prefix
                return true;
            } else if (i + matching_child.prefix.len < key.len) {
                // We need to continue searching
                if (!std.mem.eql(u8, matching_child.prefix, key[i .. i + matching_child.prefix.len])) {
                    return false;
                }
                current = matching_child;
                i += matching_child.prefix.len;
            } else {
                // Exact match of the entire key
                return std.mem.eql(u8, matching_child.prefix, key[i..]);
            }
        }

        return current.value.terminal;
    }

    pub fn get(self: *const PatriciaTrie, key: []const u8) u32 {
        var current = &self.root;
        var i: usize = 0;

        while (i < key.len) {
            var longest_prefix: usize = 0;
            var matching_child: *Node = undefined;

            for (current.children.items) |*child| {
                const common_len = commonPrefixLen(child.prefix, key[i..]);
                if (common_len > longest_prefix) {
                    longest_prefix = common_len;
                    matching_child = child;
                }
            }

            if (longest_prefix == 0) {
                return std.math.maxInt(u32);
            }

            if (i + matching_child.prefix.len > key.len) {
                // The search key is a prefix of this node's prefix
                return std.math.maxInt(u32);
            } else if (i + matching_child.prefix.len < key.len) {
                // We need to continue searching
                if (!std.mem.eql(u8, matching_child.prefix, key[i .. i + matching_child.prefix.len])) {
                    return std.math.maxInt(u32);
                }
                current = matching_child;
                i += matching_child.prefix.len;
            } else {
                // Exact match of the entire key
                if (std.mem.eql(u8, matching_child.prefix, key[i..])) {
                    if (matching_child.value.terminal) {
                        return @intCast(matching_child.value.value);
                    }
                    current = matching_child;
                }
            }
        }

        if (current.value.terminal) {
            return @intCast(current.value.value);
        }
        return std.math.maxInt(u32);
    }

    // pub fn getNumMatchingPrefix(self: *const PatriciaTrie, key: []const u8) !usize {
    // }
};


test "PatriciaTrie basic operations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var trie = try PatriciaTrie.init(allocator);
    defer trie.deinit();

    // Test insertions
    try trie.insert("hello", 0);
    try trie.insert("hell", 1);
    try trie.insert("helicopter", 2);
    try trie.insert("helipad", 3);

    try std.testing.expectEqual(4, trie.terminal_count);
    try std.testing.expect(trie.root.children.items.len > 0);
    try trie.insert("helipad", 3);
    try std.testing.expectEqual(4, trie.terminal_count);
    try trie.insert("helipay", 3);
    try std.testing.expectEqual(5, trie.terminal_count);

    // Test successful searches
    try std.testing.expect(trie.search("hel"));
    try std.testing.expect(trie.search("hello"));
    try std.testing.expect(trie.search("helicopter"));
    try std.testing.expect(trie.search("helipad"));

    try std.testing.expect(3 == trie.get("helipad"));
    try std.testing.expect(std.math.maxInt(u32) == trie.get("hel"));
    try std.testing.expect(1 == trie.get("hell"));

    // Test unsuccessful searches
    try std.testing.expect(!trie.search("helium"));
    try std.testing.expect(!trie.search("help"));
    
    // This line was incorrect before, "he" is a valid prefix
    try std.testing.expect(trie.search("he"));

    // Test prefix searches
    try std.testing.expect(trie.search("heli"));
    try std.testing.expect(trie.search("helicopt"));

    // Test empty string
    try std.testing.expect(!trie.search(""));

    // Insert and test a longer prefix
    try trie.insert("helloworld", 4);
    try std.testing.expect(trie.search("helloworl"));  // This is also a valid prefix
    try std.testing.expect(trie.search("helloworld"));

    // Insert and test a prefix of an existing word
    try trie.insert("he", 5);
    try std.testing.expect(trie.search("he"));

    // Additional tests to verify prefix behavior
    try std.testing.expect(!trie.search("a"));  // Non-existent prefix
    try trie.insert("a", 6);
    try std.testing.expect(trie.search("a"));   // Now "a" exists
    try std.testing.expect(!trie.search("b"));  // "b" still doesn't exist

    // std.debug.print("\ngetNumMatchingPrefix(\"helicopter\"): {d}\n", .{try trie.getNumMatchingPrefix("helicopter")});
}


test "PatriciaTrie benchmark" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var trie = try PatriciaTrie.init(allocator);
    defer trie.deinit();

    var keys = std.StringHashMap(usize).init(allocator);
    defer keys.deinit();

    const _N: usize = 10_000_000;
    const num_chars: usize = 3;
    const rand = std.crypto.random;
    for (0.._N) |i| {
        var key = try allocator.alloc(u8, num_chars);
        for (0..num_chars) |j| {
            key[j] = rand.int(u8);
        }
        try keys.put(key, i);
    }

    const N = keys.count();
    std.debug.print("N: {d}\n", .{N});

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

    try std.testing.expectEqual(N, trie.terminal_count);
}