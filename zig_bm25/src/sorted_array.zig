const std = @import("std");

const ScorePair = struct {
    doc_id: u32,
    score:  f32,
};

pub fn SortedScoreArray(comptime T: type) type {
    if (!@hasField(T, "doc_id") or !@hasField(T, "score")) {
        @compileError("Type " ++ @typeName(T) ++ " must have 'doc_id: u32' and 'score: f32' fields");
    }
    if (@TypeOf(@field(std.mem.zeroes(T), "doc_id")) != u32) {
        @compileError("doc_id must be of type u32");
    }
    if (@TypeOf(@field(std.mem.zeroes(T), "score")) != f32) {
        @compileError("score must be of type f32");
    }

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: []T,
        count: usize,
        capacity: usize,

        pub fn init(allocator: std.mem.Allocator, size: usize) !Self {
            return Self{
                .allocator = allocator,
                .items = try allocator.alloc(T, size + 1),
                .count = 0,
                .capacity = size,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
        }

        fn cmp(lhs: T, rhs: T) bool {
            // For a max heap, we return true if the lhs.score is less than rhs.score
            return lhs.score > rhs.score;
        }

        pub fn clear(self: *Self) void {
            self.count = 0;
        }

        pub fn resize(self: *Self, new_size: usize) void {
            if (new_size > self.capacity) {
                @panic("Cannot grow the array");
            }
            self.capacity = new_size;
        }

        fn binarySearch(self: *Self, item: T) usize {
            // TODO: Allow for common case of very many items and place starting
            // needle closer to the end.
            var low: usize = 0;
            var high: usize = self.count;

            while (low < high) {
                const mid = low + (high - low) / 2;

                if (self.items[mid].score == item.score) return mid;

                if (cmp(self.items[mid], item)) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        fn linearSearch(self: *Self, item: T) usize {
            for (0.., self.items[0..self.count]) |idx, val| {
                if (!cmp(val, item)) {
                    return idx;
                }
            }
            return self.count;
        }

        fn search(self: *Self, item: T) usize {
            if (self.count <= 64) {
                return self.linearSearch(item);
            } else {
                return self.binarySearch(item);
            }
        }

        pub fn insert(self: *Self, item: T) void {
            const insert_idx = self.search(item);

            // self.count = @min(self.count + 1, self.items.len - 1);
            self.count = @min(self.count + 1, self.capacity - 1);

            var idx: usize = self.count;
            while (idx > insert_idx) {
                self.items[idx] = self.items[idx - 1];
                idx -= 1;
            }

            self.items[insert_idx] = item;
        }

        pub fn check(self: *Self) void {
            var prev_value: f32 = 1000000.0;
            for (0.., self.items[0..self.count]) |idx, item| {
                if (item.score > prev_value) {
                    std.debug.print("IDX: {d}\n", .{idx});
                    std.debug.print("Score: {d}\n", .{item.score});
                    std.debug.print("Prev Score: {d}\n", .{prev_value});
                    @panic("Bad copy\n");
                }
                prev_value = item.score;
            }
        }
    };
}

test "sorted_arr" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var arr = try SortedScoreArray(ScorePair).init(allocator, 10);
    defer arr.deinit();

    for (0..10) |idx| {
        const item = ScorePair{ 
            .doc_id = @intCast(idx + 1), 
            .score = 1.0 - 0.1 * @as(f32, @floatFromInt(idx)) 
        };
        arr.insert(item);
        arr.check();
    }

    try std.testing.expectEqual(arr.items.len - 1, arr.count);
    try std.testing.expectEqual(10, arr.count);
    try std.testing.expectEqual(1, arr.items[0].doc_id);
    try std.testing.expectEqual(2, arr.items[1].doc_id);
    try std.testing.expectEqual(3, arr.items[2].doc_id);

    const item = ScorePair{ .doc_id = 42069, .score = 10000.0 };
    arr.insert(item);
    try std.testing.expectEqual(10, arr.count);
    try std.testing.expectEqual(42069, arr.items[0].doc_id);
    try std.testing.expectEqual(10000.0, arr.items[0].score);
    try std.testing.expectEqual(1, arr.items[1].doc_id);
}
