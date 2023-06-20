const std = @import("std");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const validate = @import("validate");
const dproxy = @import("dproxy.zig");

const Config = dproxy.Config;
const Allocator = std.mem.Allocator;

pub const App = struct {
	config: Config,
	log_http: bool,
	dbs: zuckdb.Pool,
	allocator: Allocator,
	validators: validate.Pool(void),

	pub fn init(allocator: Allocator, config: Config) !App {
		const db_path = config.db_path;
		const db = switch (zuckdb.DB.init(allocator, db_path, .{})) {
			.ok => |db| db,
			.err => |err| return dproxy.duckdbError("db.init", err, logz.err().string("path", db_path)),
		};
		errdefer db.deinit();

		var dbs = switch (zuckdb.Pool.init(db, .{.size = config.db_pool_size})) {
			.ok => |pool| pool,
			.err => |err| return dproxy.duckdbError("pool.init", err, logz.err().string("path", db_path)),
		};
		errdefer dbs.deinit();

		return .{
			.dbs = dbs,
			.config = config,
			.allocator = allocator,
			.log_http = config.log_http,
			.validators = try validate.Pool(void).init(allocator, .{}),
		};
	}

	pub fn deinit(self: *App) void {
		self.dbs.deinit();
		self.validators.deinit();
	}
};
