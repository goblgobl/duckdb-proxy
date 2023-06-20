const httpz = @import("httpz");
const builtin = @import("builtin");

pub const Config = struct {
	// For improving the uniqueness of request_id in a multi-server setup
	// The instance_id is part of the request_id, thus N instances will generate
	// distinct request_ids from each other
	instance_id: u8 = 0,

	// path to the db
	db_path: []const u8,

	// number of connections to the db to keep
	db_pool_size: u32 = 50,

	// https://github.com/ziglang/zig/issues/15091
	log_http: bool = if (builtin.is_test) false else true,

	http: httpz.Config = .{},
};
