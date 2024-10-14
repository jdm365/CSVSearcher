const std = @import("std");

pub const c = @cImport({
    @cInclude("roaring.c");
});


test "init" {
    // var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // defer arena.deinit();
    // const allocator = arena.allocator();

    std.debug.print("CREATED\n", .{});
    const r1 = c.roaring_bitmap_create();
    std.debug.print("CREATED\n", .{});

    for (0..65536) |idx| {
        c.roaring_bitmap_add(r1, @intCast(idx));
    }

    std.debug.print("CARDINALITY: {d}\n", .{c.roaring_bitmap_get_cardinality(r1)});
    c.roaring_bitmap_free(r1);
}
