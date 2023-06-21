const std = @import("std");
const validate = @import("validate");

pub const web = @import("../web.zig");
pub const dproxy = web.dproxy;

// expose handlers
const _exec = @import("exec.zig");
pub const exec = _exec.handler;

pub fn init(builder: *validate.Builder(void)) !void {
	try _exec.init(builder);
}
