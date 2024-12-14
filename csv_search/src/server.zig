const std = @import("std");
const parseRecordCSV = @import("main.zig").parseRecordCSV;
const zap = @import("zap");


var float_buf: [1000][64]u8 = undefined;

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

pub fn csvLineToJsonScore(
    allocator: std.mem.Allocator,
    csv_line: []const u8,
    term_positions: []TermPos,
    columns: std.ArrayList([]const u8),
    score: f32,
    idx: usize,
) !std.json.Value {
    var json_object = std.json.ObjectMap.init(allocator);
    errdefer json_object.deinit();

    for (0.., term_positions) |i, entry| {
        const column_name = columns.items[i];
        if (std.mem.eql(u8, "SCORE", column_name)) continue;
        const field_value = csv_line[entry.start_pos..entry.start_pos + entry.field_len];

        try json_object.put(
            column_name,
            std.json.Value{
                .string = try allocator.dupe(u8, field_value),
            },
        );
    }
    const score_str = try std.fmt.bufPrint(&float_buf[idx], "{d:.4}", .{score});
    try json_object.put(
        "SCORE",
        std.json.Value{
            .string = score_str,
        },
    );

    return std.json.Value{
        .object = json_object,
    };
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

    try columns.append("SCORE");

    const json_object_score = try csvLineToJsonScore(
        allocator,
        csv_line,
        result_positions,
        columns,
        3.4,
        0,
    );

    for (0..13) |col_idx| {
        const column_name = columns.items[col_idx];
        const field_value = json_object_score.object.get(column_name).?.string;
        std.debug.print("Column: {s}, Value: {s}\n\n", .{column_name, field_value});
    }
}

// fn on_request(r: zap.Request) void {
//     if (r.path) |the_path| {
//         std.debug.print("PATH: {s}\n", .{the_path});
//     }
// 
//     if (r.query) |the_query| {
//         std.debug.print("QUERY: {s}\n", .{the_query});
//     }
//     r.sendBody("<html><body><h1>Hello from ZAP!!!</h1></body></html>") catch return;
// }
// 
// test "client" {
//     var listener = zap.HttpListener.init(.{
//         .port = 5000,
//         .on_request = on_request,
//         .log = true,
//     });
//     try listener.listen();
// 
//     std.debug.print("\n\n\nListening on 0.0.0.0:5000\n", .{});
// 
//     // start worker threads
//     zap.start(.{
//         .threads = 1,
//         .workers = 1,
//     });
// }
