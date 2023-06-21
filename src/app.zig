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
		const db_config = config.db;
		const zuckdb_config = zuckdb.DB.Config{
			.access_mode = if (db_config.readonly) .read_only else .read_write,
			.enable_external_access = db_config.external_access,
		};

		const db = switch (zuckdb.DB.init(allocator, db_config.path, zuckdb_config)) {
			.ok => |db| db,
			.err => |err| return dproxy.duckdbError("db.init", err, logz.err().string("path", db_config.path)),
		};
		errdefer db.deinit();

		var dbs = switch (zuckdb.Pool.init(db, .{.size = db_config.pool_size})) {
			.ok => |pool| pool,
			.err => |err| return dproxy.duckdbError("pool.init", err, logz.err().string("path", db_config.path)),
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
