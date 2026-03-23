const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Import the flop module from the engine source
    const flop_mod = b.createModule(.{
        .root_source_file = b.path("../../zig/src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Server executable
    const server_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "flop", .module = flop_mod },
        },
    });

    const server = b.addExecutable(.{
        .name = "reddit-server",
        .root_module = server_mod,
    });
    b.installArtifact(server);

    const run_server = b.addRunArtifact(server);
    run_server.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_server.addArgs(args);
    }

    const run_step = b.step("run", "Run the reddit server");
    run_step.dependOn(&run_server.step);
}