const std = @import("std");
const RadixTrie = @import("radix_trie.zig").RadixTrie;

// Opaque pointer type for the trie
pub const RadixTrieHandle = *anyopaque;


// C API Functions
pub export fn radix_trie_create() ?RadixTrieHandle {
    const allocator = std.heap.c_allocator;
    
    // Create the trie
    const trie = allocator.create(RadixTrie(*anyopaque, false)) catch return null;
    trie.* = RadixTrie(*anyopaque, false).init(allocator) catch return null;
    
    return @ptrCast(trie);
}

pub export fn radix_trie_destroy(handle: ?RadixTrieHandle) void {
    if (handle) |h| {
        const trie = @as(*RadixTrie(*anyopaque, false), @ptrCast(@alignCast(h)));
        trie.deinit();
        std.heap.c_allocator.destroy(trie);
    }
}

pub export fn radix_trie_insert(
    handle: ?RadixTrieHandle,
    key_ptr: [*]const u8,
    key_len: usize,
    value: *anyopaque,
) bool {
    if (handle) |h| {
        const trie = @as(*RadixTrie(*anyopaque, false), @ptrCast(@alignCast(h)));
        const key = key_ptr[0..key_len];
        trie.insert(key, value) catch return false;
        return true;
    }
    return false;
}

pub export fn radix_trie_find(
    handle: ?RadixTrieHandle,
    key_ptr: [*]const u8,
    key_len: usize,
    value: **anyopaque,
) bool {
    if (handle) |h| {
        const trie = @as(*RadixTrie(*anyopaque, false), @ptrCast(@alignCast(h)));
        const key = key_ptr[0..key_len];
        if (trie.find(key)) |found_value| {
            value.* = found_value;
            return true;
        } else |_| {
            return false;
        }
        return true;
    }
    return false;
}

pub export fn radix_trie_get_memory_usage(handle: ?RadixTrieHandle) usize {
    if (handle) |h| {
        const trie = @as(*RadixTrie(*anyopaque, false), @ptrCast(@alignCast(h)));
        return trie.getMemoryUsage();
    }
    return 0;
}
