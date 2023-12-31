const validate = @import("validate");
const Config = @import("config.zig").Config;

// There's no facility to do initialization on startup (like Go's init), so
// we'll just hard-code this ourselves. The reason we extract this out is
// largely so that our tests can call this (done when a test context is created)
pub fn init(builder: *validate.Builder(void), config: Config) !void {
	try @import("web/sql/_sql.zig").init(builder, config.max_parameters);
	try @import("parameter.zig").init(builder);
}
