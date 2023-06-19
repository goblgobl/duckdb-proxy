// Serves two similar purposes. The first is to put misc global functions. Not
// great, but we don't have many, so not bad either.
// The other is to act as a single source of truth for types within the project.
// In most cases, if you need to reference something _within_ the project, you
// should just import dproxy and that should expose anything you might need.

pub const testing = @import("t.zig");
pub const App = @import("app.zig").App;
pub const Env = @import("env.zig").Env;
pub const Config = @import("config.zig").Config;
pub const Parameter = @import("parameter.zig").Parameter;

// Log DuckDB error.
const logz = @import("logz");
pub fn duckdbError(ctx: []const u8, err: anytype, logger: logz.Logger) error{DuckDBError} {
	defer err.deinit();
	logger.level(.Error).ctx(ctx).boolean("duckdb", true).err(err.err).string("desc", err.desc).log();
	return error.DuckDBError;
}

// Different places can return different error. I like having them all in one place.
pub const codes = struct {
	pub const INTERNAL_SERVER_ERROR_UNCAUGHT = 0;
	pub const INTERNAL_SERVER_ERROR_CAUGHT = 1;
	pub const NOT_FOUND = 2;
	pub const INVALID_JSON = 10;
	pub const VALIDATION_ERROR = 11;
};


pub const val = struct {
	pub const INVALID_SQL = 100;
	pub const UNSUPPORTED_PARAMETER_TYPE = 101;
	pub const WRONG_PARAMETER_COUNT = 102;
};
