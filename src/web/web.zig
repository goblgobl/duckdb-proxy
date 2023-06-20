const std = @import("std");
const logz = @import("logz");
const uuid = @import("uuid");
const httpz = @import("httpz");
const typed = @import("typed");
const validate = @import("validate");

const crud = @import("crud/_crud.zig");
pub const dproxy = @import("../dproxy.zig");

const App = dproxy.App;
const Env = dproxy.Env;

var _request_id: u32 = 0;

pub fn start(app: *App) !void {
	{
		var seed: u64 = undefined;
		try std.os.getrandom(std.mem.asBytes(&seed));
		var r = std.rand.DefaultPrng.init(seed);
		// request_id is allowed to have duplicates, but we'd like to minimize
		// that especially around deploys/restarts.
		_request_id = r.random().uintAtMost(u32, 10_000_000);
	}

	var server = try httpz.ServerCtx(*App, *Env).init(app.allocator, .{}, app);
	server.dispatcher(dispatcher);
	server.notFound(notFound);
	server.errorHandler(errorHandler);

	var router = server.router();
	router.post("/api/1/select", crud.select);
	router.post("/api/1/mutate", crud.mutate);
	try server.listen();
}

fn dispatcher(app: *App, action: httpz.Action(*Env), req: *httpz.Request, res: *httpz.Response) !void {
	const validator = try app.validators.acquire({});
	defer app.validators.release(validator);

	const encoded_request_id = encodeRequestId(app.config.instance_id, @atomicRmw(u32, &_request_id, .Add, 1, .SeqCst));

	var logger = logz.logger().string("$rid", &encoded_request_id).multiuse();
	defer logger.release();

	var env = Env{
		.app = app,
		.logger = logger,
		.validator = validator,
	};

	action(&env, req, res) catch |err| switch (err) {
		error.Validation => {
			res.status = 400;
			return res.json(.{
				.err = "validation error",
				.code = dproxy.codes.VALIDATION_ERROR,
				.validation = validator.errors(),
			}, .{.emit_null_optional_fields = false});
		},
		error.InvalidJson => return errors.InvalidJson.write(res),
		else => {
			const error_id = try uuid.allocHex(res.arena);
			logger.level(.Error).
				ctx("http.err").
				stringSafe("eid", error_id).
				stringSafe("m", @tagName(req.method)).
				stringSafe("p", req.url.path).
				log();

			res.status = 500;
			return res.json(.{
				.err = "internal server error",
				.code = dproxy.codes.INTERNAL_SERVER_ERROR_CAUGHT,
				.error_id = error_id,
			}, .{});
		}
	};
}

pub fn validateBody(env: *Env, req: *httpz.Request, v: *validate.Object(void)) !typed.Map {
	const body = (try req.body()) orelse {
		return error.InvalidJson;
	};

	const validator = env.validator;
	const input = try v.validateJsonS(body, validator);
	if (!validator.isValid()) {
		return error.Validation;

	}
	return input;
}

pub const Error = struct {
	code: i32,
	status: u16,
	body: []const u8,

	fn init(status: u16, comptime code: i32, comptime message: []const u8) Error {
		const body = std.fmt.comptimePrint("{{\"code\": {d}, \"err\": \"{s}\"}}", .{code, message});
		return .{
			.code = code,
			.body = body,
			.status = status,
		};
	}

	pub fn write(self: Error, res: *httpz.Response) void {
		res.status = self.status;
		res.content_type = httpz.ContentType.JSON;
		res.body = self.body;
	}
};

// bunch of static errors that we can serialize at comptime
pub const errors = struct {
	pub const ServerError = Error.init(500, dproxy.codes.INTERNAL_SERVER_ERROR_UNCAUGHT, "internal server error");
	pub const NotFound = Error.init(404, dproxy.codes.NOT_FOUND, "not found");
	pub const InvalidJson = Error.init(400, dproxy.codes.INVALID_JSON, "invalid JSON");
};

fn notFound(_: *const App, _: *httpz.Request, res: *httpz.Response) !void {
	errors.NotFound.write(res);
}

fn errorHandler(_: *const App, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
	logz.err().err(err).ctx("errorHandler").string("path", req.url.raw).log();
	errors.ServerError.write(res);
}

fn encodeRequestId(instance_id: u8, request_id: u32) [8]u8 {
	const REQUEST_ID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
	const encoded_requested_id = std.mem.asBytes(&request_id);

	var encoded: [8]u8 = undefined;
	encoded[7] = REQUEST_ID_ALPHABET[instance_id&0x1F];
	encoded[6] = REQUEST_ID_ALPHABET[(instance_id>>5|(encoded_requested_id[0]<<3))&0x1F];
	encoded[5] = REQUEST_ID_ALPHABET[(encoded_requested_id[0]>>2)&0x1F];
	encoded[4] = REQUEST_ID_ALPHABET[(encoded_requested_id[0]>>7|(encoded_requested_id[1]<<1))&0x1F];
	encoded[3] = REQUEST_ID_ALPHABET[((encoded_requested_id[1]>>4)|(encoded_requested_id[2]<<4))&0x1F];
	encoded[2] = REQUEST_ID_ALPHABET[(encoded_requested_id[2]>>1)&0x1F];
	encoded[1] = REQUEST_ID_ALPHABET[((encoded_requested_id[2]>>6)|(encoded_requested_id[3]<<2))&0x1F];
	encoded[0] = REQUEST_ID_ALPHABET[encoded_requested_id[3]>>3];
	return encoded;
}

const t = dproxy.testing;

test "dispatcher: encodeRequestId" {
	try t.expectString("AAAAAAYA", &encodeRequestId(0, 3));
	try t.expectString("AAAAABAA", &encodeRequestId(0, 4));
	try t.expectString("AAAAAAYC", &encodeRequestId(2, 3));
	try t.expectString("AAAAABAC", &encodeRequestId(2, 4));
}

test "web.dispatch: invalid json" {
	var tc = t.context(.{});
	defer tc.deinit();

	try dispatcher(tc.app, testInvalidJsonAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(400);
	try tc.web.expectJson(.{.code = 10, .err = "invalid JSON"});
}

test "web.dispatch: failed validation" {
	var tc = t.context(.{});
	defer tc.deinit();

	try dispatcher(tc.app, testValidationFailAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(400);
	try tc.web.expectJson(.{.code = 11, .validation = &.{.{.code = 322, .err = "it cannot be done"}}});
}

test "web.dispatch: generic action error" {
	t.noLogs();
	defer t.restoreLogs();

	var tc = t.context(.{});
	defer tc.deinit();

	try dispatcher(tc.app, testErrorAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(500);
	try tc.web.expectJson(.{.code = 1, .err = "internal server error"});
}

test "web.dispatch: success" {
	var tc = t.context(.{});
	defer tc.deinit();
	try dispatcher(tc.app, testSuccessAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(200);
	try tc.web.expectJson(.{.success = true, .over = 9000});
}

fn testInvalidJsonAction(_: *Env, _: *httpz.Request, _: *httpz.Response) !void {
	return error.InvalidJson;
}

fn testValidationFailAction(env: *Env, _: *httpz.Request, _: *httpz.Response) !void {
	try env.validator.add(.{.code = 322, .err = "it cannot be done"});
	return error.Validation;
}

fn testErrorAction(_: *Env, _: *httpz.Request, _: *httpz.Response) !void {
	return error.ErrorAction;
}

fn testSuccessAction(_: *Env, _: *httpz.Request, res: *httpz.Response) !void {
	return res.json(.{.success = true, .over = 9000}, .{});
}