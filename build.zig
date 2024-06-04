const std = @import("std");

const ModuleMap = std.StringArrayHashMap(*std.Build.Module);

pub fn build(b: *std.Build) !void {
	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = gpa.allocator();

	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	var modules = ModuleMap.init(allocator);
	defer modules.deinit();

	const dep_opts = .{.target = target,.optimize = optimize};

	try modules.put("zul", b.dependency("zul", dep_opts).module("zul"));
	try modules.put("logz", b.dependency("logz", dep_opts).module("logz"));
	try modules.put("httpz", b.dependency("httpz", dep_opts).module("httpz"));
	try modules.put("typed", b.dependency("typed", dep_opts).module("typed"));
	try modules.put("validate", b.dependency("validate", dep_opts).module("validate"));
	const zuckdb = b.dependency("zuckdb", dep_opts).module("zuckdb");
	try modules.put("zuckdb",  zuckdb);

	zuckdb.addRPathSpecial(".");
	zuckdb.addIncludePath(b.path("."));
	zuckdb.addLibraryPath(b.path("."));

	// setup executable
	const exe = b.addExecutable(.{
		.name = "duckdb-proxy",
		.root_source_file = b.path("src/main.zig"),
		.target = target,
		.optimize = optimize,
	});
	try addLibs(exe, modules);
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
		.root_source_file = b.path("src/main.zig"),
		.target = target,
		.optimize = optimize,
		.test_runner = b.path("test_runner.zig"),
	});

	try addLibs(tests, modules);
	const run_test = b.addRunArtifact(tests);
	run_test.has_side_effects = true;

	const test_step = b.step("test", "Run tests");
	test_step.dependOn(&run_test.step);
}

fn addLibs(step: *std.Build.Step.Compile, modules: ModuleMap) !void {
	var it = modules.iterator();
	while (it.next()) |m| {
		step.root_module.addImport(m.key_ptr.*, m.value_ptr.*);
	}
	step.linkLibC();
	step.linkSystemLibrary("duckdb");
}
