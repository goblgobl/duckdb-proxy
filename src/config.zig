const logz = @import("logz");
const httpz = @import("httpz");
const builtin = @import("builtin");

pub const Config = struct {
	const DB = struct {
		// path to the db
		path: [:0]const u8 = "db.duckdb",

	// number of connections to the db to keep
		pool_size: u32 = 50,

		// sets the enable_external_access duckdb flag
		external_access: bool = true,

		// sets the duckdb access_mode flag
		readonly: bool = false,

		// whether to try to run "describe $SQL" first on the statement, when combined
		// with readonly = true, this helps ensure only SELECT statements can
		// be executed
		describe_first: bool = false
	};

	// Put a limit on the number of allowed parameters per query
	max_parameters: ?u32 = null,

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
