const std = @import("std");

pub fn build(b: *std.Build) void {
    const target   = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zap = b.dependency("zap", .{
        .target = target,
        .optimize = optimize,
        .openssl = false, // set to true to enable TLS support
    });

    const exe = b.addExecutable(.{
        .name = "zig_bm25",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("zap", zap.module("zap"));
    exe.addIncludePath(b.path("croaring"));
    exe.addCSourceFile(.{
        .file = b.path("croaring/roaring.c"),
        .flags = &.{"-DROARING_DISABLE_AVX512"},
    });

    b.installArtifact(exe);

    // Add install command to place binary in /usr/local/bin
    // const install_cmd = b.addInstallArtifact(exe, .{.dest_dir = "/usr/local/bin/zig_bm25"});
    const install_cmd = b.addInstallArtifact(exe, .{.dest_dir = .{
        .override = .{ .custom = "/usr/local/bin/zig_bm25" },
    }});
    install_cmd.step.dependOn(b.getInstallStep());

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const install_step = b.step("install_local", "Install the app");
    install_step.dependOn(&install_cmd.step);

    const tests = b.addTest(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/root.zig"),
    });
    tests.root_module.addImport("zap", zap.module("zap"));

    const test_cmd = b.addRunArtifact(tests);
    test_cmd.step.dependOn(b.getInstallStep());
    const test_step = b.step("test", "Run the tests");
    test_step.dependOn(&test_cmd.step);
}
