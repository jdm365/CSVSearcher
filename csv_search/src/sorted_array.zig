const std = @import("std");
const vector = std.simd;

const ScorePair = struct {
    doc_id: u32,
    score:  f32,
};

pub fn SortedScoreArray(comptime T: type) type {

    return struct {
        const Self = @This();

        // TODO: Consider MultiArraylist items.
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

        inline fn cmp(lhs: T, rhs: T) bool {
            // For a max heap, we return true if the lhs.score is less than rhs.score
            return lhs.score > rhs.score;
        }

        pub inline fn clear(self: *Self) void {
            self.count = 0;
        }

        pub inline fn resize(self: *Self, new_size: usize) void {
            if (new_size > self.capacity) {
                @panic("Cannot grow the array");
            }
            self.capacity = new_size;
        }

        inline fn binarySearch(self: *Self, item: T) usize {
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

        inline fn linearSearch(self: *Self, item: T) usize {
            for (0.., self.items[0..self.count]) |idx, val| {
                if (!cmp(val, item)) {
                    return idx;
                }
            }
            return self.count;
        }

        inline fn dualSearch(self: *const Self, item: T) usize {
            const mid = @divFloor(self.count, 2);
            const min = if (cmp(self.items[0], item)) 0 else mid;
            const max = min + mid;

            for (min..max, self.items[0..self.count]) |idx, val| {
                if (!cmp(val, item)) {
                    return idx;
                }
            }
            return self.count;
        }

        fn search(self: *Self, item: T) usize {
            if (self.count <= 32) {
                return self.linearSearch(item);
            } else {
                return self.binarySearch(item);
            }
        }

        pub inline fn insert(self: *Self, item: T) void {
            const insert_idx = self.search(item);
            if (insert_idx == self.capacity) return;

            self.count = @min(self.count + 1, self.capacity);

            var idx: usize = self.count;
            while (idx > insert_idx) {
                self.items[idx] = self.items[idx - 1];
                idx -= 1;
            }

            self.items[insert_idx] = item;
        }

        pub inline fn insertCheck(self: *Self, item: T) bool {
            // Returns true if inserted item was inserted, false if not.
            const insert_idx = self.search(item);
            if (insert_idx == self.capacity) return false;

            self.count = @min(self.count + 1, self.capacity);

            var idx: usize = self.count;
            while (idx > insert_idx) {
                self.items[idx] = self.items[idx - 1];
                idx -= 1;
            }

            self.items[insert_idx] = item;

            return true;
        }

        pub inline fn getMinScore(self: *Self) f32 {
            if (self.count != self.capacity) return std.math.floatMin(f32);
            return self.items[self.count - 1].score;
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

    std.debug.print("MIN SCORE: {d}\n", .{arr.getMinScore()});

    const item = ScorePair{ .doc_id = 42069, .score = 10000.0 };
    arr.insert(item);
    try std.testing.expectEqual(10, arr.count);
    try std.testing.expectEqual(42069, arr.items[0].doc_id);
    try std.testing.expectEqual(10000.0, arr.items[0].score);
    try std.testing.expectEqual(1, arr.items[1].doc_id);
}
