const std = @import("std");
const httpz = @import("httpz");
const typed = @import("typed");
const zuckdb = @import("zuckdb");
const validate = @import("validate");
const crud = @import("_crud.zig");

const dproxy = crud.dproxy;
const Env = dproxy.Env;
const Parameter = dproxy.Parameter;
const Allocator = std.mem.Allocator;

var select_validator: *validate.Object(void) = undefined;
pub fn init(builder: *validate.Builder(void)) !void {
	select_validator = builder.object(&.{
		builder.field("sql", crud.sql_validator),
		builder.field("params", crud.params_validator),
	}, .{});
}

// Select works by taken the SQL that we're given and wrapping it in a
//   select json_array('col1', col1, 'col2', col2) from (ORIGINAL_SQL) as t;
// The benefit to this is that we can return anything that duckdb can JSON
// serialize.
// To make this work, we need to;
//   1 - Execute: describe ORIGINAL_SQL to extract the result column anmes
//   2 - Execute the wrapped select json_array(...) from (ORIGINAL_SQL)
//   3 - Glue the JSON together (into the response)
pub fn handler(env: *Env, req: *httpz.Request, res: *httpz.Response) !void {
	const input = try crud.web.validateBody(env, req, select_validator);

	var validator = env.validator;

	const aa = res.arena;
	const sql = input.get([]u8, "sql").?;
	const params = if (input.get(typed.Array, "params")) |p| p.items else &[_]typed.Value{};

	// We're going to use this for 3 separate things:
	// 1 - our describe ORIGINAL_SQL, then
	// 2 - our wrapped json_array(...) from (ORIGINAL_SQL)
	// 3 - finally, to generate the "preamble" of our response
	var buf = std.ArrayList(u8).init(aa);

	// + 500 is a guess, it really depends on (a) how many columns we're selecting
	// and (b) the length of the column names.
	try buf.ensureTotalCapacity(sql.len + 500);

	const conn = try env.app.dbs.acquire();
	defer env.app.dbs.release(conn);

	// execute describe ORIGINAL_SQL to extract the column names from the result
	const column_names = blk: {
		buf.appendSliceAssumeCapacity("describe ");
		buf.appendSliceAssumeCapacity(sql);
		buf.appendAssumeCapacity(0); // null termniate this

		const stmt = switch (conn.prepareZ(@ptrCast([:0]const u8, buf.items))) {
			.ok => |stmt| stmt,
			.err => |err| {
				defer err.deinit();
				validator.addInvalidField(.{
					.field = "sql",
					.err = try aa.dupe(u8, err.desc),
					.code = dproxy.val.INVALID_SQL,
				});
				return error.Validation;
			}
		};
		defer stmt.deinit();

		const parameter_count = stmt.numberOfParameters();
		if (parameter_count != params.len) {
			// might as well do this while we're here, no point doing a lot of work
			// if this isn't right
			return Parameter.invalidParameterCount(aa, parameter_count, params.len, validator);
		}

		// for describe, we can bind null to every placeholder and still get the
		// output that we want
		for (0..parameter_count) |i| {
			try stmt.bindDynamic(i, null);
		}

		var rows = switch (stmt.execute(null)) {
			.ok => |rows| rows,
			.err => |err| return dproxy.duckdbError("Select.describe", err, env.logger),
		};
		defer rows.deinit();

		var i: usize = 0;
		var column_names = try aa.alloc([]const u8, rows.count());
		while (try rows.next()) |row| {
			column_names[i] = try aa.dupe(u8, row.get([]u8, 0).?);
			i += 1;
		}

		break :blk column_names;
	};

	// If we're here, we have a valid SQL and likely have valid parameters
	buf.clearRetainingCapacity();
	try buf.appendSlice("select json_array(");
	for (column_names) |column_name| {
		try buf.append('"');
		try buf.appendSlice(column_name);
		try buf.appendSlice("\",");
	}

	// remove the last comma
	buf.shrinkRetainingCapacity(buf.items.len - 1);
	try buf.appendSlice(")::text from (");
	try buf.appendSlice(sql);
	try buf.appendSlice(") as t");
	try buf.append(0); // null terminate

	const stmt = switch (conn.prepareZ(@ptrCast([:0]const u8, buf.items))) {
		.ok => |stmt| stmt,
		.err => |err| return dproxy.duckdbError("Select.prepare", err, env.logger),
	};
	errdefer stmt.deinit();

	for (params, 0..) |param, i| {
		try Parameter.validateAndBind(aa, i, stmt, param, validator);
	}
	if (!validator.isValid()) {
		return error.Validation;
	}

	// Our wrapped SQL always returns a single column (a JSON serialized row, as text)
	// The zuckdb library has an optimization for when we know the # of columns
	var query_state = zuckdb.StaticState(1){};

	// executeOwned means the returns rows (or err) now own the stmt (aka
	// when we call deinit, they'll free the statement as well)
	var rows = switch (stmt.executeOwned(&query_state, true)) {
		.ok => |rows| rows,
		.err => |err| return dproxy.duckdbError("Select.exec", err, env.logger),
	};
	defer rows.deinit();

	// our response looks like: {"cols": ["c1", "c2", ...], "rows": [ [...], [...], ...]}
	buf.clearRetainingCapacity();
	try buf.appendSlice("{\n \"cols\": [");
	var writer = buf.writer();
	for (column_names) |column_name| {
		try std.json.encodeJsonString(column_name, .{}, writer);
		try buf.append(',');
	}
	// remove last comma
	buf.shrinkRetainingCapacity(buf.items.len - 1);
	try buf.appendSlice("],\n \"rows\":[\n   ");

	res.content_type = .JSON;
	try res.chunk(buf.items);
	if (try rows.next()) |first| {
		try res.chunk(first.get([]u8, 0).?);
	}
	while (try rows.next()) |row| {
		try res.chunk(",\n   ");
		try res.chunk(row.get([]u8, 0).?);
	}
	try res.chunk("\n ]\n}");
}

const t = dproxy.testing;
test "select: invalid json body" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.body("{hi");
	try t.expectError(error.InvalidJson, handler(tc.env, tc.web.req, tc.web.res));
}

test "select: invalid input" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = 32, .params = true});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.TYPE_STRING, .field = "sql"});
	try tc.expectInvalid(.{.code = validate.codes.TYPE_ARRAY, .field = "params"});
}

test "select: invalid sql" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select x", });
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = dproxy.val.INVALID_SQL, .field = "sql", .err = "Binder Error: Referenced column \"x\" not found in FROM clause!\nLINE 1: describe select x\n                        ^"});
}

test "select: wrong parameters" {
	var tc = t.context(.{});
	defer tc.deinit();

	{
		tc.web.json(.{.sql = "select $1"});
		try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = dproxy.val.WRONG_PARAMETER_COUNT, .field = "params", .err = "SQL statement requires 1 parameter, 0 were given"});
	}

	{
		// test different plural form
		tc.reset();
		tc.web.json(.{.sql = "select $1, $2", .params = .{1}});
		try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = dproxy.val.WRONG_PARAMETER_COUNT, .field = "params", .err = "SQL statement requires 2 parameters, 1 was given"});
	}
}

test "select: unsupported param type" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select $1::bit", .params = .{1}});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = dproxy.val.UNSUPPORTED_PARAMETER_TYPE, .field = "params.0", .err = "Unsupported parameter type: $1 - $unknown"});
}

test "select: invalid parameter value" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select $1::bool", .params = .{"abc"}});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.TYPE_BOOL, .field = "params.0"});
}

test "select: empty result" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select 1 as id where false", .params = .{}});
	try handler(tc.env, tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.cols = .{"id"},
		.rows = .{},
	});
}

test "select: single row" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select 'over' as say, 9000 as power", .params = .{}});
	try handler(tc.env, tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.cols = .{"say", "power"},
		.rows = .{.{"over", 9000}},
	});
}

test "select: multiple rows" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{
		.sql = "select $1::varchar as c1, $2::int as c2 union select $3::varchar, $4::int order by c1",
		.params = .{"abc", 123, "hello", 932},
	});
	try handler(tc.env, tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.cols = .{"c1", "c2"},
		.rows = .{.{"abc", 123}, .{"hello", 932}},
	});
}

// mutate.zig tests more parameter type. It does more with parameters, so
// if things work there, they should work here.