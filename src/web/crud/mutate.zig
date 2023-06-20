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

const NULL_VALUE = typed.new(null);

var mutate_validator: *validate.Object(void) = undefined;
pub fn init(builder: *validate.Builder(void)) !void {
	mutate_validator = builder.object(&.{
		builder.field("sql", crud.sql_validator),
		builder.field("params", crud.params_validator),
	}, .{});
}

pub fn handler(env: *Env, req: *httpz.Request, res: *httpz.Response) !void {
	const input = try crud.web.validateBody(env, req, mutate_validator);

	const aa = res.arena;
	const sql = input.get([]u8, "sql").?;
	const params = if (input.get(typed.Array, "params")) |p| p.items else &[_]typed.Value{};

	var validator = env.validator;

	const conn = try env.app.dbs.acquire();
	defer env.app.dbs.release(conn);

	const stmt = switch (conn.prepare(sql)) {
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
		return Parameter.invalidParameterCount(aa, parameter_count, params.len, validator);
	}

	for (params, 0..) |param, i| {
		try Parameter.validateAndBind(aa, i, stmt, param, validator);
	}
	if (!validator.isValid()) {
		return error.Validation;
	}

	var rows = switch (stmt.execute(null)) {
		.ok => |rows| rows,
		.err => |err| return dproxy.duckdbError("Mutate.run", err, env.logger),
	};
	defer rows.deinit();

	res.content_type = .JSON;

	// AFAIC, DuckDB's API is broken when trying to get the changed rows. There's
	// a duckdb_rows_changed, but it really broken. You see, internally, an insert
	// or update or delete stores the # of affected rows in the returned result.
	// But this, of course, doesn't work when the statement includes a
	// "returning...". So on insert/delete/update we can get a row back without
	// any way to tell whether it's the result of a "returning ..." or if it's
	// the internal changed row (internally called CHANGED_ROWS result).
	// What's worse is that if we call `duckdb_rows_changed`, it'll consume the
	// first row of the result, whether it's a `CHANGED_ROWS` or a real row from
	// returning ....

	// Despite the above (or maybe because of it), we're going to special-case
	// a result with a single column, of type i64 (DUCKDB_TYPE_BIGINT) where the
	// column name is "Count". This is a common case: it's the result from a
	// insert/update/delete without a "returning". It's still ambiguous: maybe
	// the statement had "returning Count"; - we can't tell. But it doesn't matter
	// even if it IS a returning, it'll be handled the same way
	if (rows.column_count == 1 and rows.column_types[0] == 5 and rows.count() == 1) {
		const column_name = rows.columnName(0);
		// column_name is a [*c]const u8, hence this unlooped comparison
		if (column_name[0] == 'C' and column_name[1] == 'o' and column_name[2] == 'u' and column_name[3] == 'n' and column_name[4] == 't' and column_name[5] == 0) {
			var optional_count: ?i64 = 0;
			if (try rows.next()) |row| {
				optional_count = row.get(i64, 0);
			}
			const count = optional_count orelse {
				res.body = "{\"cols\":[\"Count\"],\"rows\":[[null]]}";
				return;
			};

			if (count == 0) {
				// further special case count == 0 (very common)
				res.body = "{\"cols\":[\"Count\"],\"rows\":[[0]]}";
			} else if (count == 1) {
				// further special case count == 1 (very common)
				res.body = "{\"cols\":[\"Count\"],\"rows\":[[1]]}";
			} else {
				res.body = try std.fmt.allocPrint(aa, "{{\"cols\":[\"Count\"],\"rows\":[[{d}]]}}", .{count});
			}
			return;
		}
	}

	var column_types = try aa.alloc(zuckdb.ParameterType, rows.column_count);
	for (0..column_types.len) |i| {
		column_types[i] = rows.columnType(i);
	}

	var buf = std.ArrayList(u8).init(aa);
	var writer = buf.writer();

	var typed_row = try aa.alloc(typed.Value, column_types.len);

	// write our preamble
	try buf.appendSlice("{\n \"rows\":[");
	try res.chunk(buf.items);

	var row_count: usize = 0;
	if (try rows.next()) |row| {
		try translateRow(aa, row, column_types, typed_row);
		try res.chunk(try serializeRow(typed_row, "\n  ", &buf, writer));
		row_count = 1;
	}
	// convert each result row into a []typed.Value (which we can JSON serialize)
	while (try rows.next()) |row| {
		try translateRow(aa, row, column_types, typed_row);
		try res.chunk(try serializeRow(typed_row, ",\n  ", &buf, writer));
		row_count += 1;
	}

	// Only now that we've iterated through our rows do we know anything for sure
	if (row_count == 0) {
		try res.chunk("\n ],\n \"cols\":[]}");
	} else {
		buf.clearRetainingCapacity();
		try buf.appendSlice("\n ],\n \"cols\":[");
		for (0..column_types.len) |i| {
			try std.json.encodeJsonString(std.mem.span(rows.columnName(i)), .{}, writer);
			try buf.append(',');
		}
		buf.shrinkRetainingCapacity(buf.items.len - 1);
		try buf.appendSlice("]}");
		try res.chunk(buf.items);
	}
}

fn translateRow(aa: Allocator, row: zuckdb.Row, column_types: []zuckdb.ParameterType, into: []typed.Value) !void {
	for (column_types, 0..) |ctype, i| {
		const typed_value = switch (ctype) {
			.varchar => if (row.get([]u8, i)) |v| typed.new(v) else NULL_VALUE,
			.blob => blk: {
				if (row.get([]u8, i)) |v| {
					const encoder = std.base64.standard.Encoder;
					var out = try aa.alloc(u8, encoder.calcSize(v.len));
					break :blk typed.new(encoder.encode(out, v));
				} else break :blk NULL_VALUE;
			},
			.bool => if (row.get(bool, i)) |v| typed.new(v) else NULL_VALUE,
			.i8 => if (row.get(i8, i)) |v| typed.new(v) else NULL_VALUE,
			.i16 => if (row.get(i16, i)) |v| typed.new(v) else NULL_VALUE,
			.i32 => if (row.get(i32, i)) |v| typed.new(v) else NULL_VALUE,
			.i64 => if (row.get(i64, i)) |v| typed.new(v) else NULL_VALUE,
			.i128 => if (row.get(i128, i)) |v| typed.new(v) else NULL_VALUE,
			.u8 => if (row.get(u8, i)) |v| typed.new(v) else NULL_VALUE,
			.u16 => if (row.get(u16, i)) |v| typed.new(v) else NULL_VALUE,
			.u32 => if (row.get(u32, i)) |v| typed.new(v) else NULL_VALUE,
			.u64 => if (row.get(u64, i)) |v| typed.new(v) else NULL_VALUE,
			.f32 => if (row.get(f32, i)) |v| typed.new(v) else NULL_VALUE,
			.f64, .decimal => if (row.get(f64, i)) |v| typed.new(v) else NULL_VALUE,
			.uuid => if (row.get(zuckdb.UUID, i)) |v| typed.new(try aa.dupe(u8, &v)) else NULL_VALUE,
			.date => blk: {
				if (row.get(zuckdb.Date, i)) |date| {
					break :blk typed.new(typed.Date{
						.year = @intCast(i16, date.year),
						.month = @intCast(u8, date.month),
						.day = @intCast(u8, date.day),
					});
				} else break :blk NULL_VALUE;
			},
			.time => blk: {
				if (row.get(zuckdb.Time, i)) |time| {
					break :blk typed.new(typed.Time{
						.hour = @intCast(u8, time.hour),
						.min =  @intCast(u8, time.min),
						.sec =  @intCast(u8, time.sec),
					});
				} else break :blk NULL_VALUE;
			},
			.timestamp => if (row.get(i64, i)) |v| typed.new(typed.Timestamp{.micros = v}) else NULL_VALUE,
			.@"enum" => if (try row.getEnum(i)) |v| typed.new(v) else NULL_VALUE,
			else => error.UnsupportedValueType,
		};
		into[i] = typed_value catch (typed.new(try std.fmt.allocPrint(aa, "Cannot serialize: {any}", .{ctype})) catch unreachable);
	}
}

fn serializeRow(row: []typed.Value, prefix: []const u8, buf: *std.ArrayList(u8), writer: anytype) ![]const u8 {
	buf.clearRetainingCapacity();
	try buf.appendSlice(prefix);
	try std.json.stringify(row, .{}, writer);
	return buf.items;
}

const t = dproxy.testing;
test "mutate: invalid json body" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.body("{hi");
	try t.expectError(error.InvalidJson, handler(tc.env, tc.web.req, tc.web.res));
}

test "mutate: invalid input" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = 32, .params = true});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.TYPE_STRING, .field = "sql"});
	try tc.expectInvalid(.{.code = validate.codes.TYPE_ARRAY, .field = "params"});
}

test "mutate: invalid sql" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "update x", });
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = dproxy.val.INVALID_SQL, .field = "sql", .err = "Parser Error: syntax error at end of input"});
}

test "mutate: wrong parameters" {
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

test "mutate: unsupported param type" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select $1::bit", .params = .{1}});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = dproxy.val.UNSUPPORTED_PARAMETER_TYPE, .field = "params.0", .err = "Unsupported parameter type: $1 - $unknown"});
}

test "mutate: invalid parameter value" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select $1::bool", .params = .{"abc"}});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.TYPE_BOOL, .field = "params.0"});
}

test "mutate: invalid base64 for blog" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select $1::blob", .params = .{"not a blob"}});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.STRING_BASE64, .field = "params.0"});
}

test "mutate: no changes" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "update everythings set col_integer = 1 where col_varchar = $1", .params = .{"does not exist"}});
	try handler(tc.env, tc.web.req, tc.web.res);
	try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{0}}});
}

test "mutate: change with no result" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "insert into everythings (col_varchar) values ($1)", .params = .{"insert no results"}});
	try handler(tc.env, tc.web.req, tc.web.res);
	try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{1}}});

	const row = tc.getRow("select count(*) as count from everythings where col_varchar = 'insert no results'", .{}).?;
	try t.expectEqual(1, row.get(i64, "count").?);
}

test "mutate: every type" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{
		.sql =
			\\ insert into everythings (
			\\   col_tinyint,
			\\   col_smallint,
			\\   col_integer,
			\\   col_bigint,
			\\   col_hugeint,
			\\   col_utinyint,
			\\   col_usmallint,
			\\   col_uinteger,
			\\   col_ubigint,
			\\   col_real,
			\\   col_double,
			\\   col_decimal,
			\\   col_bool,
			\\   col_date,
			\\   col_time,
			\\   col_timestamp,
			\\   col_blob,
			\\   col_varchar,
			\\   col_uuid,
			\\   col_json,
			\\   col_enum
			\\ ) values (
			\\   $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			\\   $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
			\\   $21
			\\ )
			\\ returning *
		,
		.params = .{
			-32, -991, 3828, -7461123, 383821882392838192832928193,
			255, 65535, 4294967295, 18446744073709551615,
			-1.75, 3.1400009, 901.22,
			true, "2023-06-20", "13:35:29.332", 1687246572940921,
			"dGhpcyBpcyBhIGJsb2I=", "over 9000", "804b6dd4-d23b-4ea0-af2a-e3bf39bca496",
			"{\"over\":9000}", "type_b"
		}
	});
	handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
	try tc.web.expectJson(.{
		.cols = .{
			"col_tinyint",
			"col_smallint",
			"col_integer",
			"col_bigint",
			"col_hugeint",

			"col_utinyint",
			"col_usmallint",
			"col_uinteger",
			"col_ubigint",

			"col_real",
			"col_double",
			"col_decimal",

			"col_bool",
			"col_date",
			"col_time",
			"col_timestamp",

			"col_blob",
			"col_varchar",
			"col_uuid",
			"col_json",
			"col_enum"
		},
		.rows = .{.{
			-32,
			-991,
			3828,
			-7461123,
			383821882392838192832928193,

			255,
			65535,
			4294967295,
			18446744073709551615,

			-1.75,
			3.1400009,
			901.22,

			true,
			"2023-06-20",
			"13:35:29",
			1687246572940921,

			"dGhpcyBpcyBhIGJsb2I=",
			"over 9000",
			"804b6dd4-d23b-4ea0-af2a-e3bf39bca496",
			"{\"over\":9000}",
			"type_b"
		}}
	});
}

test "mutate: returning multiple rows" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{
		.sql = "insert into everythings (col_tinyint) values (1), (2), (3) returning col_tinyint",
		.params = .{}
	});
	handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
	try tc.web.expectJson(.{
		.cols = .{"col_tinyint"},
		.rows = .{.{1}, .{2}, .{3}}
	});
}

// we have special case handling for a single row returned as an i64 "Count"
test "mutate: special count case" {
	var tc = t.context(.{});
	defer tc.deinit();

	{
		tc.web.json(.{.sql = "insert into everythings (col_bigint) values (0) returning col_bigint as Count"});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{0}}});
	}

	{
		tc.reset();
		tc.web.json(.{.sql = "insert into everythings (col_bigint) values (1) returning col_bigint as \"Count\"",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{1}}});
	}

	{
		tc.reset();
		tc.web.json(.{.sql = "insert into everythings (col_bigint) values (1) returning col_bigint as \"Count\"",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{1}}});
	}

	{
		tc.reset();
		tc.web.json(.{.sql = "insert into everythings (col_bigint) values (null) returning col_bigint as \"Count\"",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{null}}});
	}
}
