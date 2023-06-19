// Test helpers.
const std = @import("std");
const logz = @import("logz");
const validate = @import("validate");
pub const web = @import("httpz").testing;
const dproxy = @import("dproxy.zig");

pub const expect = std.testing.expect;
pub const allocator = std.testing.allocator;

pub const expectEqual = std.testing.expectEqual;
pub const expectError = std.testing.expectError;
pub const expectSlice = std.testing.expectEqualSlices;
pub const expectString = std.testing.expectEqualStrings;
pub fn expectDelta(expected: anytype, actual: @TypeOf(expected), delta: @TypeOf(expected)) !void {
	try expectEqual(true, expected - delta <= actual);
	try expectEqual(true, expected + delta >= actual);
}


// We will _very_ rarely use this. Zig test doesn't have test lifecycle hooks. We
// can setup globals on startup, but we can't clean this up properly. If we use
// std.testing.allocator for these, it'll report a leak. So, we create a gpa
// within any leak reporting, and use that for the few globals that we have.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const leaking_allocator = gpa.allocator();

pub fn noLogs() void {
	logz.setup(leaking_allocator, .{.pool_size = 1, .level = .None, .output = .stderr}) catch unreachable;
}

pub fn restoreLogs() void {
	logz.setup(leaking_allocator, .{.pool_size = 2, .level = .Error, .output = .stderr}) catch unreachable;
}

pub fn setup() void {
	restoreLogs();

	var builder = validate.Builder(void).init(leaking_allocator) catch unreachable;
	@import("init.zig").init(&builder) catch unreachable;
}

pub fn context(_: Context.Config) *Context {
	var arena = allocator.create(std.heap.ArenaAllocator) catch unreachable;
	arena.* = std.heap.ArenaAllocator.init(allocator);

	var aa = arena.allocator();
	const app = aa.create(dproxy.App) catch unreachable;
	app.* = dproxy.App.init(allocator, .{
		.db_pool_size = 2,
		.db_path = "tests/db.duckdb",
	}) catch unreachable;

	const env = aa.create(dproxy.Env) catch unreachable;
	env.* = dproxy.Env{
		.app = app,
		.logger = logz.logger().multiuse(),
		.validator = app.validators.acquire({}) catch unreachable,
	};

	var ctx = allocator.create(Context) catch unreachable;
	ctx.* = .{
		._arena = arena,
		.app = app,
		.env = env,
		.arena = aa,
		.web = web.init(.{}),
	};
	return ctx;
}

pub const Context = struct {
	_arena: *std.heap.ArenaAllocator,
	app: *dproxy.App,
	env: *dproxy.Env,
	web: web.Testing,
	arena: std.mem.Allocator,

	const Config = struct {
	};

	pub fn deinit(self: *Context) void {
		self.env.logger.release();
		self.app.validators.release(self.env.validator);

		self.web.deinit();
		self.app.deinit();

		self._arena.deinit();
		allocator.destroy(self._arena);
		allocator.destroy(self);
	}

	pub fn expectInvalid(self: Context, expectation: anytype) !void {
		return validate.testing.expectInvalid(expectation, self.env.validator);
	}
};
