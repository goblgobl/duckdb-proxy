const std = @import("std");
const validate = @import("validate");

pub const web = @import("../web.zig");
pub const dproxy = web.dproxy;

// expose handlers
const _select = @import("select.zig");
pub const select = _select.handler;

pub var sql_validator: *validate.String(void) = undefined;
pub var params_validator: *validate.Array(void) = undefined;

pub fn init(builder: *validate.Builder(void)) !void {
	sql_validator = builder.string(.{
		.min = 1,
		.max = 10_000,
		.required = true,
	});
	params_validator = builder.array(null, .{});

	try _select.init(builder);
}
