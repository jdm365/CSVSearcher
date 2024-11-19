const std = @import("std");

pub fn build(b: *std.Build) void {
    const target   = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zap = b.dependency("zap", .{
        .target = target,
        .optimize = optimize,
        .openssl = false, // set to true to enable TLS support
    });

    const lib = b.addStaticLibrary(.{
        .name = "csv_search",
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.root_module.addImport("zap", zap.module("zap"));
    b.installArtifact(lib);

    const exe = b.addExecutable(.{
        .name = "csv_search",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("zap", zap.module("zap"));

    b.installArtifact(exe);

    // Add install command to place binary in /usr/local/bin
    // const install_cmd = b.addInstallArtifact(exe, .{.dest_dir = "/usr/local/bin/csv_search"});
    const install_cmd = b.addInstallArtifact(exe, .{.dest_dir = .{
        .override = .{ .custom = "/usr/local/bin/csv_search" },
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


    const radix_lib = b.addSharedLibrary(.{
        .name = "radix_trie",
        .root_source_file = b.path("src/radix_bindings.zig"),
        .target = target,
        .optimize = optimize,
    });
    radix_lib.linkLibC();
    b.installArtifact(radix_lib);
    
    // Install header file
    // const header_install = b.addInstallFileWithDir(
        // b.path("include/radix.h"),
        // b.path("include"),
        // b.path("src/radix.h"),
    // );
    const header_install = b.addInstallFileWithDir(
        // .{ .path = "src/radix.h" },
        b.path("src/radix.h"),
        .{ .custom = "include" },
        "radix.h"
    );
    
    // Make header installation part of the default install step
    b.getInstallStep().dependOn(&header_install.step);
}
