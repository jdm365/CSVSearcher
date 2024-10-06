const std = @import("std");

pub const bitfield_u32 = packed struct (u32) {
    terminal: bool,
    value: u31,
};

const InsertPath = enum(u2) {
    split,
    make_terminal,
    traverse,
    create,
};


const Node = extern struct {
    // num_bytes_prefix is the total number of bytes used by the prefix.
    // Non-byte aligned prefixes are allowed, so to get the differ
    value: bitfield_u32,   // 4
    num_bits_prefix: u32,  // 8
    prefix: SS_ptr,        // 16

    left_child:  ?*Node,   // 24
    right_child: ?*Node,   // 32

    const SS_ptr = extern union {
        Inline: [8]u8,
        ptr: [*]u8,

        fn accessPrefix(self: *const SS_ptr, index: usize, is_inline: bool) u8 {
            if (is_inline) return self.Inline[index];
            return self.ptr[index];
        }
    };

    fn init(allocator: std.mem.Allocator) !*Node {
        const node = try allocator.create(Node);
        node.* = Node{
            .value = bitfield_u32{
                .terminal = false,
                .value = 0,
            },
            .num_bits_prefix = 0,
            .prefix = SS_ptr{
                .Inline = undefined,
            },
            .left_child = null,
            .right_child = null,
        };
        return node;
    }

    fn matchPrefix(
        self: *const Node, 
        prefix: []const u8, 
        start_bit: *usize,
        diff_bit: *u8,
        ) !bool {
        std.debug.assert(self.num_bits_prefix > 0);

        const self_bits = self.num_bits_prefix;
        const self_bytes = (self_bits / 8) + @as(u32, @intFromBool(self_bits % 8 != 0));

        const start_byte  = start_bit.* / 8;
        // const shift_len: u3 = @intCast(7 - (start_bit.* % 8));
        const shift_len: u3 = @intCast(start_bit.* % 8);

        const bits_rem_key  = (prefix.len * 8) - start_bit.*;

        const is_inline = (self_bytes <= 8);

        if (self_bits > bits_rem_key) {
            // Self key longer than current prefix. Match not possible.
            // Move start bit to end.
            start_bit.* = prefix.len * 8;
            return false;
        }

        // Must match the entire current node's prefix or return false.
        var idx: usize = 0;
        var prefix_byte: u8 = 0;
        var self_byte:   u8 = 0;
        var xor:         u8 = 0;
        var bit_idx:     u3 = 0;

        for (0..self_bytes - 1) |_i| {
            const i = _i + start_byte;

            prefix_byte = (prefix[i] << shift_len) | (prefix[i + 1] >> (7 - shift_len));
            self_byte = self.prefix.accessPrefix(idx, is_inline);

            if (prefix_byte != self_byte) {
                xor = prefix_byte ^ self_byte;
                bit_idx = @intCast(@clz(xor));
                start_bit.* += bit_idx;
                // diff_bit.* = xor & (@as(u8, 1) << (7 - bit_idx));
                diff_bit.* = xor & (@as(u8, 1) << bit_idx);
                return false;
            }

            idx += 1;
            start_bit.* += 8;
        }

        // Do final byte.
        const final_byte_idx = start_byte + self_bytes - 1;
        prefix_byte = prefix[final_byte_idx] << shift_len;
        self_byte = self.prefix.accessPrefix(idx, is_inline);

        xor = prefix_byte ^ self_byte;
        const leading_zeroes = @clz(xor);
        start_bit.* += leading_zeroes;
        // diff_bit.* = xor & (@as(u8, 1) << (7 - bit_idx));
        diff_bit.* = xor & (@as(u8, 1) << @intCast(leading_zeroes % 8));

        return (xor == 0);
    }

    fn setPrefix(
        self: *Node, 
        allocator: std.mem.Allocator, 
        prefix: []const u8, 
        start_bit: usize,
        num_bits: usize
        ) !void {
        const num_bytes = (num_bits / 8) + @intFromBool(num_bits % 8 != 0);
        const shift_len: u3 = @intCast(7 - start_bit % 8);
        const start_byte = start_bit / 8;

        self.num_bits_prefix = @intCast(num_bits);

        if (num_bytes > 8) {
            self.prefix.ptr = @ptrCast(try allocator.alloc(u8, num_bytes));
            for (0.., prefix[start_byte..start_byte + num_bytes]) |idx, current_byte| {
                var next_byte = if (idx == num_bytes - 1) 0 else prefix[idx + 1];
                if (shift_len != 0) next_byte >>= (7 - shift_len);

                const new_byte: u8 = (current_byte << shift_len) | next_byte;
                self.prefix.ptr[idx] = new_byte;
            }
            return;
        }

        self.prefix.Inline = undefined;
        for (0..8) |i| {
            self.prefix.Inline[i] = 0;
        }

        const native_endian = @import("builtin").target.cpu.arch.endian();
        const end_byte = @min(start_byte + 8, prefix.len);
        for (0.., start_byte..end_byte) |i, byte_idx| {
            self.prefix.Inline[i] = prefix[byte_idx];
        }

        var u64_cast_prefix: u64 = std.mem.readInt(
            u64, 
            &self.prefix.Inline,
            native_endian,
            ) << shift_len;
        @memcpy(@as([*]u8, @ptrCast(&u64_cast_prefix)), &self.prefix.Inline);
    }


    fn setPrefixPtr(
        self: *Node, 
        allocator: std.mem.Allocator, 
        prefix: [*]const u8,
        start_bit: usize,
        num_bits: usize
        ) !void {
        const num_bytes = (num_bits / 8) + @intFromBool(num_bits % 8 != 0);
        const shift_len: u3 = @intCast(7 - start_bit % 8);
        const start_byte = start_bit / 8;

        self.num_bits_prefix = @intCast(num_bits);

        if (num_bytes > 8) {
            self.prefix.ptr = @ptrCast(try allocator.alloc(u8, num_bytes));
            for (0.., prefix[start_byte..start_byte + num_bytes]) |idx, current_byte| {
                var next_byte = if (idx == num_bytes - 1) 0 else prefix[idx + 1];
                if (shift_len != 0) next_byte >>= (7 - shift_len);

                const new_byte: u8 = (current_byte << shift_len) | next_byte;
                self.prefix.ptr[idx] = new_byte;
            }
            return;
        }

        self.prefix.Inline = undefined;
        for (0..8) |i| {
            self.prefix.Inline[i] = 0;
        }

        const native_endian = @import("builtin").target.cpu.arch.endian();
        const end_byte = start_byte + num_bytes;
        for (0.., start_byte..end_byte) |i, byte_idx| {
            self.prefix.Inline[i] = prefix[byte_idx];
        }

        var u64_cast_prefix: u64 = std.mem.readInt(
            u64, 
            &self.prefix.Inline,
            native_endian,
            ) << shift_len;
        @memcpy(@as([*]u8, @ptrCast(&u64_cast_prefix)), &self.prefix.Inline);
    }

    fn getDiffBit(
        self: *Node,
        key: []const u8,
        start_bit: *usize,
        diff_bit: *u8,
        ) InsertPath {

        ///////////////////////////////////////////////////////////////////////////////////
        // FOUR CASES TO HANDLE:
        // 1. Matched entire key, value not finished.
        //    - If matching child (0|1) exists, traverse. Set current node to child.
        //    - If no matching child, create new node. Write Value. RETURN.
        //    - SWITCH LABEL(s): traverse / create
        //
        // 2. Matched entire key, value finished.
        //    - Matched node. Overwrite Value. RETURN.
        //    - SWITCH LABEL(s): make_terminal
        //
        // 3. Matched entire value, key not finished.
        //    - Split node. 
        //    - Create new child node with remaining prefix.
        //    - SWITCH LABEL(s): create
        //
        // 4. Matched partial value, key not finished.
        //    - Split node.
        //    - Truncate current node's prefix to bit before differing value.
        //    - Create new child node with remaining prefix.
        //    - SWITCH LABEL(s): split
        ///////////////////////////////////////////////////////////////////////////////////

        const key_size = (key.len * 8) - start_bit.*;
        var idx: usize = 0;

        std.debug.assert(key_size > 0);

        const is_inline = (self.num_bits_prefix <= 64);

        if (key_size < self.num_bits_prefix) {
            // Only (2. make_terminal) and (3. split) possible.
            const num_bits = key_size;

            while (idx < num_bits) {
                const key_byte_idx: u3 = @intCast(start_bit.* / 8);
                const key_bit_idx: u3  = @intCast(start_bit.* % 8);
                const key_bit  = key[key_byte_idx] & (@as(u8, 1) << key_bit_idx);

                const self_byte_idx = idx / 8;
                const self_bit_idx: u3 = @intCast(idx % 8);
                const self_bit = self.prefix.accessPrefix(self_byte_idx, is_inline) & (@as(u8, 1) << self_bit_idx);

                if (self_bit != key_bit) {
                    diff_bit.* = key_bit;

                    // Partial prefix match. Split node.
                    return InsertPath.split;
                }

                start_bit.* += 1;
                idx         += 1;
            }

            // Full prefix match, but current node's prefix not complete. Traverse.
            return InsertPath.split;

        } else if (key_size == self.num_bits_prefix) {
            // Only (2. make_terminal) and (3. split) possible.
            const num_bits = key_size;

            while (idx < num_bits) {
                const key_byte_idx: u3 = @intCast(start_bit.* / 8);
                const key_bit_idx: u3  = @intCast(start_bit.* % 8);
                const key_bit  = key[key_byte_idx] & (@as(u8, 1) << key_bit_idx);

                const self_byte_idx = idx / 8;
                const self_bit_idx: u3 = @intCast(idx % 8);
                const self_bit = self.prefix.accessPrefix(self_byte_idx, is_inline) & (@as(u8, 1) << self_bit_idx);

                if (self_bit != key_bit) {
                    diff_bit.* = key_bit;
                    return InsertPath.split;
                }

                start_bit.* += 1;
                idx         += 1;
            }

            return InsertPath.make_terminal;

        } else {
            const num_bits = self.num_bits_prefix;

            while (idx < num_bits) {
                const key_byte_idx: u3 = @intCast(start_bit.* / 8);
                const key_bit_idx: u3  = @intCast(start_bit.* % 8);
                const key_bit  = key[key_byte_idx] & (@as(u8, 1) << key_bit_idx);


                const self_byte_idx = idx / 8;
                const self_bit_idx: u3 = @intCast(idx % 8);
                const self_bit = self.prefix.accessPrefix(self_byte_idx, is_inline) & (@as(u8, 1) << self_bit_idx);

                if (self_bit != key_bit) {
                    diff_bit.* = key_bit;
                    return InsertPath.split;
                }

                start_bit.* += 1;
                idx         += 1;
            }

            return InsertPath.traverse;
        }
    }
};

const PatriciaTrie = struct {
    root: ?*Node,
    allocator: std.mem.Allocator,
    mem_usage: usize,
    terminal_count: usize,

    pub fn init(allocator: std.mem.Allocator) !PatriciaTrie {
        const root = try Node.init(allocator);
        return PatriciaTrie{
            .root = root,
            .allocator = allocator,
            .mem_usage = 0,
            .terminal_count = 0,
        };
    }

    pub fn insert(self: *PatriciaTrie, key: []const u8, value: u32) !void {
        // Insert node.
        // Find first differing bit in key.
        // Traverse prefix trie.
        // If new prefix found insert it.
        // If key is prefix of existing prefix, split node, create new node.
        // If key is already in trie, make terminal if not and return.
        var current = if (key[0] & 0x80 == 0x80) self.root.?.right_child else self.root.?.left_child;
        var i: usize = 0;

        if (self.terminal_count == 0) {
            self.terminal_count += 1;

            if (key[0] & 0x80 == 0x80) {
                self.root.?.right_child = try Node.init(self.allocator);
                current = self.root.?.right_child;
            } else {
                self.root.?.left_child = try Node.init(self.allocator);
                current = self.root.?.left_child;
            }

            current.?.value = bitfield_u32{
                .terminal = true,
                .value = @intCast(value),
            };
            try current.?.setPrefix(self.allocator, key, 0, key.len * 8);
            return;
        }

        var diff_bit: u8 = 0;
        while (i < key.len * 8) {
            const start_bit = i;

            switch (current.?.getDiffBit(key, &i, &diff_bit)) {
                InsertPath.split => {
                    const matching_bits: u32 = @intCast(i - start_bit);
                    std.debug.assert(matching_bits > 0);

                    // Create new node to be child of current node.
                    var new_node = try Node.init(self.allocator);
                    new_node.value = bitfield_u32{
                        .value = @intCast(value),
                        .terminal = true,
                    };
                    std.debug.print("CURRENT BITS:  {d}\n", .{current.?.num_bits_prefix});
                    std.debug.print("MATCHING BITS: {d}\n\n", .{matching_bits});

                    // TODO: Fix if there are no matching bits.
                    if (i == key.len * 8) {
                        // Matched entire key and still current node's prefix remains.
                        // Create new node to hold remainder of current node's prefix.
                        if (current.?.num_bits_prefix > 64) {
                            try new_node.setPrefixPtr(
                                self.allocator, 
                                current.?.prefix.ptr, 
                                matching_bits,
                                current.?.num_bits_prefix - matching_bits,
                                );
                        } else {
                            try new_node.setPrefix(
                                self.allocator, 
                                &current.?.prefix.Inline, 
                                matching_bits,
                                current.?.num_bits_prefix - matching_bits,
                                );
                        }
                    } else {
                        // Matched partial key.
                        // Create new node to hold remainder of key.
                        try new_node.setPrefix(
                            self.allocator, 
                            key, 
                            i,
                            (key.len * 8) - i
                            );
                    }

                    current.?.num_bits_prefix = matching_bits;
                    // current.?.value.terminal = false;

                    if (diff_bit == 1) {
                        current.?.right_child = new_node;
                    } else {
                        current.?.left_child = new_node;
                    }

                    self.mem_usage += 32;
                    if (key.len > 8) self.mem_usage += key.len + 8;
                    self.terminal_count += 1;

                    std.debug.assert(current.?.num_bits_prefix > 0);
                    std.debug.assert(new_node.num_bits_prefix > 0);

                    return;
                },
                InsertPath.make_terminal => {
                    self.terminal_count += @intFromBool(!current.?.value.terminal);
                    current.?.value = bitfield_u32{
                        .value = @intCast(value),
                        .terminal = true,
                    };
                    return;
                },
                InsertPath.traverse => {
                    if (diff_bit == 1) {
                        if (current.?.right_child != null) {
                            current = current.?.right_child;
                            continue;
                        }

                        current.?.right_child = try Node.init(self.allocator);
                        current = current.?.right_child;
                    } else {
                        if (current.?.left_child != null) {
                            current = current.?.left_child;
                            continue;
                        }

                        current.?.left_child = try Node.init(self.allocator);
                        current = current.?.left_child;
                    }

                    // Create new node with remaining prefix and return.
                    current.?.value = bitfield_u32{
                        .value = @intCast(value),
                        .terminal = true,
                    };
                    current.?.num_bits_prefix = @intCast((key.len * 8) - i);
                    std.debug.assert(current.?.num_bits_prefix > 0);

                    try current.?.setPrefix(self.allocator, key, i, current.?.num_bits_prefix);
                    self.terminal_count += 1;
                    return;
                },
            }
        }
    }

    pub fn get(self: *const PatriciaTrie, key: []const u8) u32 {
        var idx: usize = 0;
        var current: ?*Node = if (key[0] & 0x80 == 0x80) self.root.?.right_child else self.root.?.left_child;
        var diff_bit: u8 = 0;

        while (idx < key.len * 8) {
            if (current == null) return std.math.maxInt(u32);
            std.debug.assert(current != self.root);
            std.debug.assert(current.?.num_bits_prefix > 0);

            // False means prefix differed from given value. No match.
            if (!try current.?.matchPrefix(key, &idx, &diff_bit)) return std.math.maxInt(u32);

            if (idx == key.len * 8) break;

            std.debug.print("DIFF BIT: {d}\n", .{diff_bit});
            current = if (diff_bit == 1) current.?.right_child else current.?.left_child;
        }

        if (current.?.value.terminal) return current.?.value.value;
        return std.math.maxInt(u32);
    }
};

test "ops" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var trie = try PatriciaTrie.init(allocator);

    // Test insertions
    try trie.insert("hello", 0);
    try trie.insert("hell", 1);
    try trie.insert("helicopter", 2);
    try trie.insert("helipad", 3);


    try std.testing.expectEqual(4, trie.terminal_count);
    try trie.insert("helipad", 3);
    try std.testing.expectEqual(4, trie.terminal_count);
    try trie.insert("helipay", 3);
    try std.testing.expectEqual(5, trie.terminal_count);

    try std.testing.expectEqual(std.math.maxInt(u32), trie.get("hel"));
    try std.testing.expectEqual(0, trie.get("hello"));
    try std.testing.expectEqual(2, trie.get("helicopter"));
    try std.testing.expectEqual(3, trie.get("helipad"));

    try std.testing.expectEqual(1, trie.get("hell"));

    try std.testing.expectEqual(std.math.maxInt(u32), trie.get("helium"));
    try std.testing.expectEqual(std.math.maxInt(u32), trie.get("help"));
    
    try std.testing.expectEqual(std.math.maxInt(u32), trie.get(""));

    try trie.insert("helloworld", 4);
    try std.testing.expectEqual(std.math.maxInt(u32), trie.get("helloworl"));
    try std.testing.expectEqual(4, trie.get("helloworld"));
}


test "bench" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var keys = std.StringHashMap(usize).init(allocator);
    defer keys.deinit();

    var trie = try PatriciaTrie.init(allocator);

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
