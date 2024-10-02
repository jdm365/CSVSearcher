const std = @import("std");
const parseRecordCSV = @import("main.zig").parseRecordCSV;


pub const TermPos = struct {
    start_pos: u32,
    field_len: u32,
};

pub fn csvLineToJson(
    allocator: std.mem.Allocator,
    csv_line: []const u8,
    term_positions: []TermPos,
    columns: std.ArrayList([]const u8),
) !std.json.Value {
    var json_object = std.json.ObjectMap.init(allocator);
    errdefer json_object.deinit();

    for (0.., term_positions) |idx, entry| {
        const field_value = csv_line[entry.start_pos..entry.start_pos + entry.field_len];
        const column_name = columns.items[idx];

        try json_object.put(
            column_name,
            std.json.Value{
                .string = try allocator.dupe(u8, field_value),
            },
        );
    }

    return std.json.Value{
        .object = json_object,
    };
}

pub fn serve() void {
    const read_buffer: [4096]u8 = undefined;
    const conn   = std.http.Connection;
    const server = std.http.Server.init(conn, read_buffer);
}


test "csv_parse" {
    const csv_line = "26859,13859,1,1,WoM27813813,006,Under My Skin (You Go To My Head (Set One)),02:44,David McAlmont,You_Go_To_My_Head_(Set_One),2005,,";

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result_positions = try allocator.alloc(TermPos, 12);
    defer allocator.free(result_positions);

    try parseRecordCSV(csv_line, result_positions);

    var columns = std.ArrayList([]const u8).init(allocator);
    defer columns.deinit();

    try columns.appendSlice(&[_][]const u8{
        "id",
        "artist_id",
        "album_id",
        "track_id",
        "track_name",
        "track_number",
        "track_title",
        "track_duration",
        "artist_name",
        "track_slug",
        "release_year",
        "track_genre",
    });

    for (0..12) |col_idx| {
        std.debug.print("start_pos: {d}, field_len: {d}\n", .{result_positions[col_idx].start_pos, result_positions[col_idx].field_len});
        std.debug.print("Term: {s}\n", .{csv_line[result_positions[col_idx].start_pos..result_positions[col_idx].start_pos + result_positions[col_idx].field_len]});
    }

    const json_object = try csvLineToJson(
        allocator,
        csv_line,
        result_positions,
        columns,
    );

    for (0..12) |col_idx| {
        const column_name = columns.items[col_idx];
        const field_value = json_object.object.get(column_name).?.string;
        std.debug.print("Column: {s}, Value: {s}\n", .{column_name, field_value});
    }
}
