const std = @import("std");

const ScorePair = struct {
    doc_id: u32,
    score:  f32,
};

const SortedScoreArray = struct {
    allocator: std.mem.Allocator,
    items: []ScorePair,
    count: usize,

    pub fn init(allocator: std.mem.Allocator, size: usize) !SortedScoreArray {
        return SortedScoreArray{
            .allocator = allocator,
            .items = try allocator.alloc(ScorePair, size + 1),
            .count = 0,
        };
    }

    pub fn deinit(self: *SortedScoreArray) void {
        self.allocator.free(self.items);
    }

    fn cmp(lhs: ScorePair, rhs: ScorePair) bool {
        // For a max heap, we return true if the lhs.score is less than rhs.score
        return lhs.score > rhs.score;
    }

    fn binarySearch(self: *SortedScoreArray, item: ScorePair) usize {
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

    pub fn insert(self: *SortedScoreArray, item: ScorePair) void {
        const insert_idx = self.binarySearch(item);

        self.count = @min(self.count + 1, self.items.len - 1);

        var idx: usize = self.count;
        while (idx > insert_idx) {
            self.items[idx] = self.items[idx - 1];
            idx -= 1;
        }

        self.items[insert_idx] = item;
    }

    pub fn check(self: *SortedScoreArray) void {
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

test "sorted_arr" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var arr = try SortedScoreArray.init(allocator, 10);
    defer arr.deinit();

    for (0..10) |idx| {
        const item = ScorePair{ .doc_id = @intCast(idx + 1), .score = 1.0 - 0.1 * @as(f32, @floatFromInt(idx)) };
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
