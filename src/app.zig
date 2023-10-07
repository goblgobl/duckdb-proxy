const std = @import("std");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const validate = @import("validate");
const dproxy = @import("dproxy.zig");
const BufferPool = @import("buffer").Pool;

const Config = dproxy.Config;
const Allocator = std.mem.Allocator;

pub const App = struct {
	config: Config,
	log_http: bool,
	with_wrap: bool,
	max_limit: ?[]const u8,
	dbs: zuckdb.Pool,
	allocator: Allocator,
	buffer_pool: BufferPool,
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

		var max_limit: ?[]const u8 = null;
		if (config.max_limit) |l| {
			// no reason to do this more than once!
			max_limit = try std.fmt.allocPrint(allocator, " limit {d}", .{l});
		}

		return .{
			.dbs = dbs,
			.config = config,
			.allocator = allocator,
			.max_limit = max_limit,
			.log_http = config.log_http,
			.with_wrap = config.with_wrap,
			.validators = try validate.Pool(void).init(allocator, .{}),
			.buffer_pool = try BufferPool.init(allocator, db_config.pool_size, 2048),
		};
	}

	pub fn deinit(self: *App) void {
		self.dbs.deinit();
		self.validators.deinit();
		self.buffer_pool.deinit();
		if (self.max_limit) |l| {
			self.allocator.free(l);
		}
	}
};
