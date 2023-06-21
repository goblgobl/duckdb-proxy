const std = @import("std");

const ModuleMap = std.StringArrayHashMap(*std.Build.Module);
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub fn build(b: *std.Build) !void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	const allocator = gpa.allocator();

	var modules = ModuleMap.init(allocator);
	defer modules.deinit();

	const dep_opts = .{.target = target,.optimize = optimize};

	const zuckdb_package = b.dependency("zuckdb", dep_opts);

	try modules.put("logz", b.dependency("logz", dep_opts).module("logz"));
	try modules.put("httpz", b.dependency("httpz", dep_opts).module("httpz"));

	try modules.put("typed", b.dependency("typed", dep_opts).module("typed"));
	try modules.put("validate", b.dependency("validate", dep_opts).module("validate"));

	try modules.put("zuckdb", zuckdb_package.module("zuckdb"));
	// try modules.put("zuckdb", b.addModule("zuckdb", .{
	// 	.source_file = .{.path = "../zuckdb.zig/src/zuckdb.zig"},
	// 	.dependencies = &.{.{.name = "typed", .module = modules.get("typed").?}},
	// }));

	// local libraries
	try modules.put("uuid", b.addModule("uuid", .{.source_file = .{.path = "lib/uuid/uuid.zig"}}));

	// setup executable
	const exe = b.addExecutable(.{
		.name = "duckdb-proxy",
		.root_source_file = .{ .path = "src/main.zig" },
		.target = target,
		.optimize = optimize,
	});
	try addLibs(exe, modules, zuckdb_package);
	b.installArtifact(exe);

	const run_cmd = b.addRunArtifact(exe);
	run_cmd.step.dependOn(b.getInstallStep());
	if (b.args) |args| {
		run_cmd.addArgs(args);
	}

	// setup tests
	const run_step = b.step("run", "Run the app");
	run_step.dependOn(&run_cmd.step);

	const tests = b.addTest(.{
		.root_source_file = .{ .path = "src/main.zig" },
		.target = target,
		.optimize = optimize,
	});

	try addLibs(tests, modules, zuckdb_package);
	const run_test = b.addRunArtifact(tests);
	run_test.has_side_effects = true;

	const test_step = b.step("test", "Run tests");
	test_step.dependOn(&run_test.step);
}

fn addLibs(step: *std.Build.CompileStep, modules: ModuleMap, zuckdb_package: anytype) !void {
	var it = modules.iterator();
	while (it.next()) |m| {
		step.addModule(m.key_ptr.*, m.value_ptr.*);
	}

	// this cannot be the right way to do this...
	const zuckdb_include = try std.fs.path.join(gpa.allocator(), &[_][]const u8{zuckdb_package.builder.build_root.path.?, "lib"});
	step.addIncludePath(zuckdb_include);

	step.linkLibC();
	step.linkSystemLibrary("duckdb");
	step.addRPath("lib/duckdb");
	step.addLibraryPath("lib/duckdb");
}
