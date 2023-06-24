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
	const yazap = @import("yazap");

	var app = yazap.App.init(allocator, "duckdb-proxy", "HTTP Proxy for DuckDB");
	defer app.deinit();

	var cmd = app.rootCommand();
	try cmd.addArg(yazap.Arg.positional("DB_PATH", null, null));
	try cmd.addArg(yazap.Arg.singleValueOption("port", 'l', "Port to listen on (default: 8012)"));
	try cmd.addArg(yazap.Arg.singleValueOption("address", 'a', "Address to bind to (default: 127.0.0.1)"));
	try cmd.addArg(yazap.Arg.booleanOption("readonly", null, "Opens the database in readonly mode"));
	try cmd.addArg(yazap.Arg.booleanOption("with_wrap", null, "Executes the provided SQL as \"with _ as ($SQL) select * from _\", significantly limiting the type of queries that can be run"));
	try cmd.addArg(yazap.Arg.singleValueOption("max_limit", null, "Force a \"limit N\" on all SQL, this automatically enables --with_wrap"));
	try cmd.addArg(yazap.Arg.booleanOption("external_access", null, "Enables the duckdb enable_external_access configuration"));
	try cmd.addArg(yazap.Arg.singleValueOption("pool_size", null, "Number of connections to keep open (default: 50)"));
	try cmd.addArg(yazap.Arg.singleValueOption("max_params", null, "Maximum number of parameters allowed per request (default: no limit)"));
	try cmd.addArg(yazap.Arg.singleValueOption("max_request_size", null, "Maximum size of the request body (default: 65536)"));
	try cmd.addArg(yazap.Arg.singleValueOptionWithValidValues("log_level", null, "Log level to use (default: INFO), see also log_http)", &[_][]const u8{"info", "warn", "error", "fatal", "none"}));
	try cmd.addArg(yazap.Arg.booleanOption("log_http", null, "Log http request lines, works independently of log_level"));
	try cmd.addArg(yazap.Arg.singleValueOption("cors_origin", null, "Enables CORS response headers using the specified origin"));
	try cmd.addArg(yazap.Arg.booleanOption("version", 'v', "Print the version and exit"));

	const stdout = std.io.getStdOut().writer();
	const args = app.parseProcess() catch {
		try stdout.print("Use duckdb-proxy --help\n", .{});
		return null;
	};

	if (args.containsArg("version")) {
		try std.io.getStdOut().writer().print(dproxy.version, .{});
		return null;
	}

	var pool_size: u16 = 50;
	var log_level = logz.Level.Info;
	var max_limit: ?u32 = null;
	var max_parameters: ?u32 = null;
	var cors: ?httpz.Config.CORS = null;
	var port: u16 = 8012;
	var address: []const u8 = "127.0.0.1";
	var max_request_size: u32 = 65536;

	if (args.getSingleValue("pool_size")) |value| {
		pool_size = std.fmt.parseInt(u16, value, 10) catch {
			try stdout.print("pool_size must be a positive integer\n", .{});
			return null;
		};
		if (pool_size == 0) {
			try stdout.print("pool_size must be greater than 0\n", .{});
			return null;
		}
	}

	if (args.getSingleValue("max_limit")) |value| {
		max_limit = std.fmt.parseInt(u32, value, 10) catch {
			try stdout.print("max_limit must be a positive integer\n", .{});
			return null;
		};
	}

	if (args.getSingleValue("max_params")) |value| {
		max_parameters = std.fmt.parseInt(u32, value, 10) catch {
			try stdout.print("max_params must be a positive integer\n", .{});
			return null;
		};
	}

	if (args.getSingleValue("max_request_size")) |value| {
		max_request_size = std.fmt.parseInt(u32, value, 10) catch {
			try stdout.print("max_request_size must be a positive integer\n", .{});
			return null;
		};
	}


	if (args.getSingleValue("cors_origin")) |value| {
		cors = .{
			.origin = try allocator.dupe(u8, value),
			.headers = "content-type",
			.max_age = "7200",
		};
	}

	if (args.getSingleValue("port")) |value| {
		port = std.fmt.parseInt(u16, value, 10) catch {
			try stdout.print("port must be a positive integer\n", .{});
			return null;
		};
	}

	if (args.getSingleValue("address")) |value| {
		address = try allocator.dupe(u8, value);
	}

	if (args.getSingleValue("log_level")) |value| {
		log_level = logz.Level.parse(value) orelse {
			try stdout.print("invalid log_level value\n", .{});
			return null;
		};
	}

	return .{
		.db = .{
			.path = if (args.getSingleValue("DB_PATH")) |v| try allocator.dupeZ(u8, v) else "db.duckdb",
			.pool_size = pool_size,
			.readonly = args.containsArg("readonly"),
			.external_access = args.containsArg("external_access")
		},
		.http = .{
			.port = port,
			.address = address,
			.cors = cors,
			.pool_size = pool_size,
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
		.with_wrap = args.containsArg("with_wrap") or max_limit != null,
		.logger = .{.level = log_level},
		.log_http = args.containsArg("log_http"),
	};
}

test {
	dproxy.testing.setup();
	std.testing.refAllDecls(@This());
}
