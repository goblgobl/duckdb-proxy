const std = @import("std");
const zul = @import("zul");
const httpz = @import("httpz");
const typed = @import("typed");
const zuckdb = @import("zuckdb");
const validate = @import("validate");
const Buffer = @import("buffer").Buffer;

const base = @import("_sql.zig");

const dproxy = base.dproxy;
const Env = dproxy.Env;
const Parameter = dproxy.Parameter;
const Allocator = std.mem.Allocator;

var exec_validator: *validate.Object(void) = undefined;
pub fn init(builder: *validate.Builder(void), max_parameters: ?u32) !void {
	exec_validator = builder.object(&.{
		builder.field("sql", builder.string(.{
			.min = 1,
			.max = 10_000,
			.required = true,
		})),
		builder.field("params",  builder.array(null, .{.max = if (max_parameters) |max| @intCast(max) else null})),
	}, .{});
}

pub fn handler(env: *Env, req: *httpz.Request, res: *httpz.Response) !void {
	const input = try base.web.validateBody(env, req, exec_validator);

	const aa = res.arena;
	const sql = input.get([]u8, "sql").?;
	const params = if (input.get(typed.Array, "params")) |p| p.items else &[_]typed.Value{};

	var validator = env.validator;

	const app = env.app;

	// The zuckdb library is going to dupeZ the SQL to get a null-terminated string
	// We might as well do this with our arena allocator.
	var sb = try app.buffer_pool.acquire();
	defer app.buffer_pool.release(sb);

	const sql_string = switch (app.with_wrap) {
		false => sql,
		true => blk: {
			try sb.ensureTotalCapacity(sql.len + 50);
			sb.writeAssumeCapacity("with _dproxy as (");
			// if we're wrapping, we need to strip any trailing ; to keep it a valid SQL
			sb.writeAssumeCapacity(stripTrailingSemicolon(sql));
			sb.writeAssumeCapacity(") select * from _dproxy");
			if (app.max_limit) |l| {
				sb.writeAssumeCapacity(l);
			}
			break :blk sb.string();
		},
	};

	var conn = try app.dbs.acquire();
	defer app.dbs.release(conn);

	var stmt = conn.prepare(sql_string, .{}) catch |err| switch (err) {
		error.DuckDBError => {
				validator.addInvalidField(.{
				.field = "sql",
				.err = if (conn.err) |ce| try aa.dupe(u8, ce) else "invalid sql",
				.code = dproxy.val.INVALID_SQL,
			});
			return error.Validation;
		},
		else => return err,
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

	var rows = stmt.execute(null) catch |err| switch (err) {
		error.DuckDBError => {
				validator.addInvalidField(.{
				.field = "sql",
				.err = if (conn.err) |ce| try aa.dupe(u8, ce) else "invalid sql",
				.code = dproxy.val.INVALID_SQL,
			});
			return error.Validation;
		},
		else => return err,
	};
	defer rows.deinit();

	res.content_type = .JSON;

	// AFAIC, DuckDB's API is broken when trying to get the changed rows. There's
	// a duckdb_rows_changed, but it's really broken. You see, internally, an insert
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
				optional_count = row.get(?i64, 0);
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

	sb.clearRetainingCapacity();
	const writer = sb.writer();

	const typed_row = try aa.alloc(typed.Value, column_types.len);

	// write our preamble
	try sb.write("{\n \"cols\": [");
	for (0..column_types.len) |i| {
		try std.json.encodeJsonString(std.mem.span(rows.columnName(i)), .{}, writer);
		try sb.writeByte(',');
	}
	// strip out the last comma
	sb.truncate(1);
	try sb.write("],\n \"rows\": [");
	try res.chunk(sb.string());

	var row_count: usize = 0;
	if (try rows.next()) |row| {
		try translateRow(aa, &row, column_types, typed_row);
		try res.chunk(try serializeRow(typed_row, "\n   ", sb, writer));
		row_count = 1;
	}
	// convert each result row into a []typed.Value (which we can JSON serialize)
	while (try rows.next()) |row| {
		try translateRow(aa, &row, column_types, typed_row);
		try res.chunk(try serializeRow(typed_row, ",\n   ", sb, writer));
		row_count += 1;
	}

	try res.chunk("\n ]\n}");
}

fn translateRow(aa: Allocator, row: *const zuckdb.Row, parameter_types: []zuckdb.ParameterType, into: []typed.Value) !void {
	for (parameter_types, 0..) |parameter_type, i| {
		if (row.isNull(i)) {
			into[i] = .{.null = {}};
			continue;
		}

		into[i] = switch (parameter_type) {
			.list => blk: {
				var list = row.lazyList(i) orelse break :blk .{.null = {}};
				var typed_list = typed.Array.init(aa);
				try typed_list.ensureTotalCapacity(list.len);
				for (0..list.len) |list_index| {
					typed_list.appendAssumeCapacity(try translateScalar(aa, &list, list.type, list_index));
				}
				break :blk .{.array = typed_list};
			},
			else => try translateScalar(aa, row, parameter_type, i),
		};
	}
}

// src can either be a zuckdb.Row or a zuckdb.LazyList
fn translateScalar(aa: Allocator, src: anytype, parameter_type: zuckdb.ParameterType, i: usize) !typed.Value {
	if (src.isNull(i)) {
		return.{.null = {}};
	}

	switch (parameter_type) {
		.varchar => return .{.string = src.get([]const u8, i)},
		.blob => {
			const v = src.get([]const u8, i);
			const encoder = std.base64.standard.Encoder;
			const out = try aa.alloc(u8, encoder.calcSize(v.len));
			return .{.string = encoder.encode(out, v)};
		},
		.bool => return .{.bool = src.get(bool, i)},
		.i8 => return .{.i8 = src.get(i8, i)},
		.i16 => return .{.i16 = src.get(i16, i)},
		.i32 => return .{.i32 = src.get(i32, i)},
		.i64 => return .{.i64 = src.get(i64, i)},
		.i128 => return .{.i128 = src.get(i128, i)},
		.u8 => return .{.u8 = src.get(u8, i)},
		.u16 => return .{.u16 = src.get(u16, i)},
		.u32 => return .{.u32 = src.get(u32, i)},
		.u64 => return .{.u64 = src.get(u64, i)},
		.f32 => return .{.f32 = src.get(f32, i)},
		.f64, .decimal => return .{.f64 = src.get(f64, i)},
		.uuid => return .{.string = try aa.dupe(u8, &src.get(zuckdb.UUID, i))},
		.date => {
			const date = src.get(zuckdb.Date, i);
			return .{.date = .{
				.year = @intCast(date.year),
				.month = @intCast(date.month),
				.day = @intCast(date.day),
			}};
		},
		.time => {
			const time = src.get(zuckdb.Time, i);
			return .{.time = .{
				.hour = @intCast(time.hour),
				.min =  @intCast(time.min),
				.sec =  @intCast(time.sec),
			}};
		},
		.timestamp => return .{.timestamp = .{.micros = src.get(i64, i)}},
		.@"enum" => return .{.string = try src.get(zuckdb.Enum, i).rowCache()},
		.interval => {
			const interval = src.get(zuckdb.Interval, i);
			var map = typed.Map.init(aa);
			try map.putAll(.{.months = interval.months, .days = interval.days, .micros = interval.micros});
			return .{.map = map};
		},
		.bitstring => return .{.string = try zuckdb.bitToString(aa, src.get([]u8, i))},
		else => return .{.string = try std.fmt.allocPrint(aa, "Cannot serialize: {any}", .{parameter_type})},
	}
}


fn serializeRow(row: []typed.Value, prefix: []const u8, sb: *zul.StringBuilder, writer: anytype) ![]const u8 {
	sb.clearRetainingCapacity();
	try sb.write(prefix);
	try std.json.stringify(row, .{}, writer);
	return sb.string();
}

fn stripTrailingSemicolon(sql: []const u8) []const u8 {
	var i : usize = sql.len-1;
	while (i >= 0) : (i -= 1) {
		if (!std.ascii.isWhitespace(sql[i])) break;
	}

	while (i >= 0) : (i -= 1) {
		if (sql[i] != ';') break;
	}
	return sql[0..i+1];
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
	defer row.deinit();
	try t.expectEqual(1, row.get(i64, 0));
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
			\\   col_enum,
			\\   col_list_integer,
			\\   col_list_varchar,
			\\   col_interval,
			\\   col_bitstring
			\\ ) values (
			\\   $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			\\   $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
			\\   $21, [1, null, 2], ['over', '9000', '!', '!1'],
			\\   $22, $23
			\\ )
			\\ returning *
		,
		.params = .{
			-32, -991, 3828, -7461123, 383821882392838192832928193,
			255, 65535, 4294967295, 18446744073709551615,
			-1.75, 3.1400009, 901.22,
			true, "2023-06-20", "13:35:29.332", 1687246572940921,
			"dGhpcyBpcyBhIGJsb2I=", "over 9000", "804b6dd4-d23b-4ea0-af2a-e3bf39bca496",
			"{\"over\":9000}", "type_b", "45 days", "001010011101"
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
			"col_enum",
			"col_list_integer",
			"col_list_varchar",
			"col_interval",
			"col_bitstring"
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
			"type_b",

			&.{1, null, 2},
			&.{"over", "9000", "!", "!1"},

			.{.months = 0, .days = 45, .micros = 0},
			"001010011101"
		}}
	});
}

// above tested interval as a string, but we also accept it as an object
test "mutate: interval as object" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{
		.sql = "insert into everythings (col_interval) values ($1), ($2), ($3) returning col_interval",
		.params = .{.{.months = 0}, .{.months = 33, .days = 91, .micros = 3232958}, .{.days = 5}},
	});
	handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
	try tc.web.expectJson(.{
		.cols = .{"col_interval"},
		.rows = .{
			.{.{.months = 0, .days = 0, .micros = 0}},
			.{.{.months = 33, .days = 91, .micros = 3232958}},
			.{.{.months = 0, .days = 5, .micros = 0}},
		}
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

test "mutate: with_wrap" {
	var tc = t.context(.{.with_wrap = true});
	defer tc.deinit();

	{
		// a select statement can be executed, no problem
		tc.web.json(.{.sql = "select 1 as x",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"x"}, .rows = .{.{1}}});
	}

	{
		// semicolon ok
		tc.reset();
		tc.web.json(.{.sql = "select 1 as x;",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"x"}, .rows = .{.{1}}});
	}

	{
		// semicolon with spacing
		tc.reset();
		tc.web.json(.{.sql = "select 1 as x ;  \t\n ",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"x"}, .rows = .{.{1}}});
	}

	{
		// nested CTE, can lah!
		tc.reset();
		tc.web.json(.{.sql = "with x as (select 3 as y) select * from x union all select 4",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"y"}, .rows = .{.{3}, .{4}}});
	}

	{
		// other statements cannot
		tc.reset();
		tc.web.json(.{.sql = "  \n  DEscribe  select 1 as x"});
		try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = dproxy.val.INVALID_SQL, .field = "sql"});
	}

	{
		// other statements cannot
		tc.reset();
		tc.web.json(.{.sql = "delete from everythings"});
		try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = dproxy.val.INVALID_SQL, .field = "sql"});
	}

	{
		// non describable
		tc.reset();
		tc.web.json(.{.sql = "begin"});
		try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = dproxy.val.INVALID_SQL, .field = "sql"});
	}
}

test "mutate: max_limit" {
	var tc = t.context(.{.max_limit = 2});
	defer tc.deinit();

	{
		tc.web.json(.{.sql = "select 1 as x union all select 2",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"x"}, .rows = .{.{1}, .{2}}});
	}

	{
		tc.reset();
		tc.web.json(.{.sql = "select 1 as x union all select 2 union all select 3",});
		handler(tc.env, tc.web.req, tc.web.res) catch |err| tc.handlerError(err);
		try tc.web.expectJson(.{.cols = .{"x"}, .rows = .{.{1}, .{2}}});
	}
}
