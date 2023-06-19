pub const Config = struct {
	// For improving the uniqueness of request_id in a multi-server setup
	// The instance_id is part of the request_id, thus N instances will generate
	// distinct request_ids from each other
	instance_id: u8 = 0,
	db_path: []const u8,
	db_pool_size: u32 = 50,
};
