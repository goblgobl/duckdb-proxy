const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const validate = @import("validate");

const init = @import("init.zig");
const web = @import("web/web.zig");
const dproxy = @import("dproxy.zig");

const Allocator = std.mem.Allocator;

pub fn main() !void {
	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = gpa.allocator();

	const config = (try parseArgs(allocator)) orelse std.os.exit(1);

	try logz.setup(allocator, config.logger);
	logz.info().ctx("Log.setup").
		stringSafe("level", @tagName(logz.level())).
		string("note", "alter via --log_level=LEVEL flag").
		log();

	var app = try dproxy.App.init(allocator, config);
	var validation_builder = try validate.Builder(void).init(allocator);
	try init.init(&validation_builder, app.config);

	try web.start(&app);
}

fn parseArgs(allocator: Allocator) !?dproxy.Config {
	const httpz = @import("httpz");

	var args = try zul.CommandLineArgs.parse(allocator);
	defer args.deinit();

	const stdout = std.io.getStdOut().writer();

	if (args.contains("version") or args.contains("v")) {
		try stdout.print(dproxy.version, .{});
		std.os.exit(0);
	}

	if (args.contains("help") or args.contains("h")) {
		try stdout.print("./duckdb-proxy [OPTS] DB_PATH\n\n", .{});
		try stdout.print("OPTS:\n", .{});
		try stdout.print("  --port <PORT>\n\tPort to listen on (default: 8012)\n\n", .{});
		try stdout.print("  --address <ADDRESS>\n\tAddress to bind to (default: 127.0.0.1)\n\n", .{});
		try stdout.print("  --readonly\n\tOpens the database in readonly mode\n\n", .{});
		try stdout.print("  --with_wrap\n\tExecutes the provided SQL as \"with _ as ($SQL) select * from _\",\n\tsignificantly limiting the type of queries that can be run\n\n", .{});
		try stdout.print("  --max_limit\n\tForce a \"limit N\" on all SQL, this automatically enables --with_wrap\n\n", .{});
		try stdout.print("  --external_access\n\tEnables the duckdb enable_external_access configuration\n\n", .{});
		try stdout.print("  --pool_size <SIZE>\n\tNumber of connections to keep open (default: 50)\n\n", .{});
		try stdout.print("  --max_params <COUNT>\n\tMaximum number of parameters allowed per request (default: no limit)\n\n", .{});
		try stdout.print("  --max_request_size <SIZE>\n\tMaximum size of the request body (default: 65536)\n\n", .{});
		try stdout.print("  --log_level <LEVEL>\n\tLog level to use (default: INFO).\n\tValid values are: info, warn, error, fatal, none. See also log_http)\n\n", .{});
		try stdout.print("  --log_http\n\tLog http request lines, works independently of log_level\n\n", .{});
		try stdout.print("  --cors_origin <ORIGIN>\n\tEnables CORS response headers using the specified origin\n\n", .{});
		try stdout.print("  -v, --version\n\tPrint the version and exit\n\n", .{});
		std.os.exit(0);
	}

	var pool_size: u16 = 50;
	var log_level = logz.Level.Info;
	var max_limit: ?u32 = null;
	var max_parameters: ?u32 = null;
	var cors: ?httpz.Config.CORS = null;
	var port: u16 = 8012;
	var address: []const u8 = "127.0.0.1";
	var max_request_size: u32 = 65536;

	if (args.get("pool_size")) |value| {
		pool_size = std.fmt.parseInt(u16, value, 10) catch {
			try stdout.print("pool_size must be a positive integer\n", .{});
			return null;
		};
		if (pool_size == 0) {
			try stdout.print("pool_size must be greater than 0\n", .{});
			return null;
		}
	}

	if (args.get("max_limit")) |value| {
		max_limit = std.fmt.parseInt(u32, value, 10) catch {
			try stdout.print("max_limit must be a positive integer\n", .{});
			return null;
		};
	}

	if (args.get("max_params")) |value| {
		max_parameters = std.fmt.parseInt(u32, value, 10) catch {
			try stdout.print("max_params must be a positive integer\n", .{});
			return null;
		};
	}

	if (args.get("max_request_size")) |value| {
		max_request_size = std.fmt.parseInt(u32, value, 10) catch {
			try stdout.print("max_request_size must be a positive integer\n", .{});
			return null;
		};
	}

	if (args.get("cors_origin")) |value| {
		cors = .{
			.origin = try allocator.dupe(u8, value),
			.headers = "content-type",
			.max_age = "7200",
		};
	}

	if (args.get("port")) |value| {
		port = std.fmt.parseInt(u16, value, 10) catch {
			try stdout.print("port must be a positive integer\n", .{});
			return null;
		};
	}

	if (args.get("address")) |value| {
		address = try allocator.dupe(u8, value);
	}

	if (args.get("log_level")) |value| {
		log_level = logz.Level.parse(value) orelse {
			try stdout.print("invalid log_level value\n", .{});
			return null;
		};
	}

	return .{
		.db = .{
			.path = if (args.tail.len == 1) try allocator.dupeZ(u8, args.tail[0]) else "db.duckdb",
			.pool_size = pool_size,
			.readonly = args.contains("readonly"),
			.external_access = args.contains("external_access")
		},
		.http = .{
			.port = port,
			.address = address,
			.cors = cors,
			.response = .{
				// we use chunked responses, so don't a response buffer
				.body_buffer_size = 2048
			},
			.request  = .{
				.max_body_size = max_request_size,
			},
		},
		.max_limit = max_limit,
		.max_parameters = max_parameters,
		.with_wrap = args.contains("with_wrap") or max_limit != null,
		.logger = .{.level = log_level},
		.log_http = args.contains("log_http"),
	};
}

test {
	dproxy.testing.setup();
	std.testing.refAllDecls(@This());
}
