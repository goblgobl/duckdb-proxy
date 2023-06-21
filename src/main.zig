const std = @import("std");
const logz = @import("logz");
const validate = @import("validate");

const init = @import("init.zig");
const web = @import("web/web.zig");
const dproxy = @import("dproxy.zig");

const Allocator = std.mem.Allocator;

pub fn main() !void {
	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = gpa.allocator();

	try logz.setup(allocator, .{});
	logz.info().ctx("Log.setup").
		stringSafe("level", @tagName(logz.level())).
		string("note", "alter via --log_level=LEVEL flag").
		log();

	var app = try dproxy.App.init(allocator, .{
		.db = .{
			.readonly = true,
			.path = "/tmp/pondz/default/main.duckdb",
		},
		.max_parameters = 2,
	});
	var validation_builder = try validate.Builder(void).init(allocator);
	try init.init(&validation_builder, &app);

	try web.start(&app);
}

test {
	dproxy.testing.setup();
	std.testing.refAllDecls(@This());
}
