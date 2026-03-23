const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Depend on the flop engine from ../../zig/
    const flop_dep = b.dependency("flop", .{
        .target = target,
        .optimize = optimize,
    });
    const flop_mod = flop_dep.module("flop");

    // Build the finance benchmark executable
    const exe = b.addExecutable(.{
        .name = "finance-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "flop", .module = flop_mod },
            },
        }),
    });

    b.installArtifact(exe);

    // Run step
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the finance benchmark server");
    run_step.dependOn(&run_cmd.step);
}