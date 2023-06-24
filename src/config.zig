const logz = @import("logz");
const httpz = @import("httpz");
const builtin = @import("builtin");

pub const Config = struct {
	const DB = struct {
		// path to the db
		path: [:0]const u8 = "db.duckdb",

	// number of connections to the db to keep
		pool_size: u16 = 50,

	// sets the enable_external_access duckdb flag
		external_access: bool = true,

		// sets the duckdb access_mode flag
		readonly: bool = false,
	};

	// Put a limit on the number of allowed parameters per query
	max_parameters: ?u32 = null,

	// whether to wrap the SQL in a "with _ as ($SQL) select * from _", this restricts
	// the types of SQL statements that can be executed.
	with_wrap: bool = false,

	// forces a limit on the number of returned rows, when set, implies with_sql_wrapper.
	max_limit: ?u32 = 0,

	// For improving the uniqueness of request_id in a multi-server setup
	// The instance_id is part of the request_id, thus N instances will generate
	// distinct request_ids from each other
	instance_id: u8 = 0,

	logger: logz.Config = logz.Config{},

	// https://github.com/ziglang/zig/issues/15091
	log_http: bool = if (builtin.is_test) false else true,

	http: httpz.Config = .{},

	db: DB = .{},
};
