// Test helpers.
const std = @import("std");
const logz = @import("logz");
const typed = @import("typed");
const zuckdb = @import("zuckdb");
const validate = @import("validate");
pub const web = @import("httpz").testing;
const dproxy = @import("dproxy.zig");

pub usingnamespace @import("zul").testing;
pub const allocator = std.testing.allocator;

// We will _very_ rarely use this. Zig test doesn't have test lifecycle hooks. We
// can setup globals on startup, but we can't clean this up properly. If we use
// std.testing.allocator for these, it'll report a leak. So, we create a gpa
// without any leak reporting, and use that for the few globals that we have.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const leaking_allocator = gpa.allocator();

pub fn noLogs() void {
	logz.setup(leaking_allocator, .{.pool_size = 1, .level = .None, .output = .stderr}) catch unreachable;
}

pub fn restoreLogs() void {
	logz.setup(leaking_allocator, .{.pool_size = 2, .level = .Error, .output = .stderr}) catch unreachable;
}

// Run once, in main.zig's nameless test {...} block
pub fn setup() void {
	restoreLogs();

	var builder = validate.Builder(void).init(leaking_allocator) catch unreachable;
	@import("init.zig").init(&builder, .{}) catch unreachable;

	{
		std.fs.cwd().deleteFile("tests/db.duckdb") catch |err| switch (err) {
			error.FileNotFound => {},
			else => {
				std.debug.print("Failed to delete 'tests/db.duckdb' - {any}\n", .{err});
				unreachable;
			}
		};

		// create some dummy data
		const db = zuckdb.DB.init(allocator, "tests/db.duckdb", .{}) catch unreachable;
		defer db.deinit();

		var conn = db.conn() catch unreachable;
		defer conn.deinit();

		_ = conn.exec("create type everything_type as enum ('type_a', 'type_b')", .{}) catch unreachable;

		_ = conn.exec(
			\\ create table everythings (
			\\   col_tinyint tinyint,
			\\   col_smallint smallint,
			\\   col_integer integer,
			\\   col_bigint bigint,
			\\   col_hugeint hugeint,
			\\   col_utinyint utinyint,
			\\   col_usmallint usmallint,
			\\   col_uinteger uinteger,
			\\   col_ubigint ubigint,
			\\   col_real real,
			\\   col_double double,
			\\   col_decimal decimal(5, 2),
			\\   col_bool bool,
			\\   col_date date,
			\\   col_time time,
			\\   col_timestamp timestamp,
			\\   col_blob blob,
			\\   col_varchar varchar,
			\\   col_uuid uuid,
			\\   col_json json,
			\\   col_enum everything_type,
			\\   col_list_integer integer[],
			\\   col_list_varchar varchar[],
			\\   col_interval interval,
			\\   col_bitstring bit
			\\ )
		, .{}) catch unreachable;
	}
}

// The test context contains an *App and *Env that we can use in our tests.
// It also includes a httpz.testing instance, so that we can easily test http
// handlers. It uses and exposes an arena allocator so that, any memory we need
// to allocate within the test itself, doesn't have to be micro-managed.
pub fn context(config: Context.Config) *Context {
	var arena = allocator.create(std.heap.ArenaAllocator) catch unreachable;
	arena.* = std.heap.ArenaAllocator.init(allocator);

	var aa = arena.allocator();
	const app = aa.create(dproxy.App) catch unreachable;
	app.* = dproxy.App.init(allocator, .{
		.db = .{
			.pool_size = 2,
			.path = "tests/db.duckdb",
		},
		.max_limit = config.max_limit,
		.with_wrap = config.with_wrap or config.max_limit != null,
	}) catch unreachable;

	const env = aa.create(dproxy.Env) catch unreachable;
	env.* = dproxy.Env{
		.app = app,
		.logger = logz.logger().multiuse(),
		.validator = app.validators.acquire({}) catch unreachable,
	};

	const ctx = allocator.create(Context) catch unreachable;
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
		with_wrap: bool = false,
		max_limit: ?u32 = null,
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

	pub fn reset(self: *Context) void {
		self.env.validator.reset();
		self.web.deinit();
		self.web = web.init(.{});
	}

	pub fn getRow(self: *Context, sql: [:0]const u8, values: anytype) ?zuckdb.OwningRow {
		var conn = self.app.dbs.acquire() catch unreachable;
		defer self.app.dbs.release(conn);

		return conn.row(sql, values) catch |err| {
			std.log.err("GetRow: {s}\nErr: {s}", .{sql, conn.err orelse @errorName(err)});
			unreachable;
		} orelse return null;
	}

	pub fn handlerError(self: *Context, err: anyerror) void {
		switch (err) {
			error.Validation => self.env.validator.dump() catch unreachable,
			else => std.debug.print("Unexpected handler error: {any}:\n", .{err}),
		}
		unreachable;
	}
};
