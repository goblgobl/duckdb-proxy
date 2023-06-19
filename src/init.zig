const validate = @import("validate");

// There's no facility to do initialization on startup (like Go's init), so
// we'll just hard-code this ourselves. The reason we extract this out is
// largely so that our tests can call this (done when a test context is created)
pub fn init(builder: *validate.Builder(void)) !void {
	try @import("web/crud/_crud.zig").init(builder);
	try @import("parameter.zig").init(builder);
}
