const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
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
	const sql = input.get("sql").?.string;
	const params = if (input.get("params")) |p| p.array.items else &[_]typed.Value{};

	var validator = env.validator;

	const app = env.app;

	// The zuckdb library is going to dupeZ the SQL to get a null-terminated string
	// We might as well do this with our arena allocator.
	var buf = try app.buffer_pool.acquire();
	defer buf.release();

	const sql_string = switch (app.with_wrap) {
		false => sql,
		true => blk: {
			try buf.ensureTotalCapacity(sql.len + 50);
			buf.writeAssumeCapacity("with _dproxy as (");
			// if we're wrapping, we need to strip any trailing ; to keep it a valid SQL
			buf.writeAssumeCapacity(stripTrailingSemicolon(sql));
			buf.writeAssumeCapacity(") select * from _dproxy");
			if (app.max_limit) |l| {
				buf.writeAssumeCapacity(l);
			}
			break :blk buf.string();
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

	var rows = stmt.query(null) catch |err| switch (err) {
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
	if (isSingleI64Result(&rows) == true) {
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

	const logger = env.logger;

	res.content_type = .JSON;
	buf.clearRetainingCapacity();
	const writer = buf.writer();
	const vectors = rows.vectors;
	try buf.write("{\n \"cols\": [");
	for (0..vectors.len) |i| {
		try std.json.encodeJsonString(std.mem.span(rows.columnName(i)), .{}, writer);
		try buf.writeByte(',');
	}
	// strip out the last comma
	buf.truncate(1);
	try buf.write("],\n \"types\": [");

	for (vectors) |*vector| {
		try buf.writeByte('"');
		try vector.writeType(writer);
		try buf.write("\",");
	}
	buf.truncate(1);

	const arena = res.arena;
	try buf.write("],\n \"rows\": [");
	if (try rows.next()) |first_row| {
		try buf.write("\n  [");
		try writeRow(arena, &first_row, buf, vectors, logger);

		var row_count: usize = 1;
		while (try rows.next()) |row| {
			buf.writeAssumeCapacity("],\n  [");
			try writeRow(arena, &row, buf, vectors, logger);
			if (@mod(row_count, 50) == 0) {
				try res.chunk(buf.string());
				buf.clearRetainingCapacity();
			}
			row_count += 1;
		}
		try buf.writeByte(']');
	}
	try buf.write("\n]\n}");
	try res.chunk(buf.string());
}

fn writeRow(allocator: Allocator, row: *const zuckdb.Row, buf: *zul.StringBuilder, vectors: []zuckdb.Vector, logger: logz.Logger) !void {
	const writer = buf.writer();

	for (vectors, 0..) |*vector, i| {
		switch (vector.type) {
			.list => |list_vector| {
				const list = row.lazyList(i) orelse {
					try buf.write("null,");
					continue;
				};
				if (list.len == 0) {
					try buf.write("[],");
					continue;
				}

				const child_type = list_vector.child;
				try buf.writeByte('[');
				for (0..list.len) |list_index| {
					try translateScalar(allocator, &list, child_type, list_index, writer, logger);
					try buf.writeByte(',');
				}
				// overwrite the last trailing comma
				buf.truncate(1);
				try buf.write("],");
			},
			.scalar => |s| {
				try translateScalar(allocator, row, s, i, writer, logger);
				try buf.writeByte(',');
			}
		}
	}
	// overwrite the last trailing comma
	buf.truncate(1);
}

// src can either be a zuckdb.Row or a zuckdb.LazyList
fn translateScalar(allocator: Allocator, src: anytype, column_type: zuckdb.Vector.Type.Scalar, i: usize, writer: anytype, logger: logz.Logger) !void {
	if (src.isNull(i)) {
		return writer.writeAll("null");
	}

	switch (column_type) {
		.decimal => try std.fmt.format(writer, "{d}", .{src.get(f64, i)}),
		.@"enum" => try std.json.encodeJsonString(try src.get(zuckdb.Enum, i).rowCache(), .{}, writer),
		.simple => |s| switch (s) {
			zuckdb.c.DUCKDB_TYPE_VARCHAR => try std.json.encodeJsonString(src.get([]const u8, i), .{}, writer),
			zuckdb.c.DUCKDB_TYPE_BOOLEAN => try writer.writeAll(if (src.get(bool, i)) "true" else "false"),
			zuckdb.c.DUCKDB_TYPE_TINYINT => try std.fmt.formatInt(src.get(i8, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_SMALLINT => try std.fmt.formatInt(src.get(i16, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_INTEGER => try std.fmt.formatInt(src.get(i32, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_BIGINT => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_HUGEINT => try std.fmt.formatInt(src.get(i128, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UTINYINT => try std.fmt.formatInt(src.get(u8, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_USMALLINT => try std.fmt.formatInt(src.get(u16, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UINTEGER => try std.fmt.formatInt(src.get(u32, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UBIGINT => try std.fmt.formatInt(src.get(u64, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UHUGEINT => try std.fmt.formatInt(src.get(u128, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_FLOAT => try std.fmt.format(writer, "{d}", .{src.get(f32, i)}),
			zuckdb.c.DUCKDB_TYPE_DOUBLE => try std.fmt.format(writer, "{d}", .{src.get(f64, i)}),
			zuckdb.c.DUCKDB_TYPE_UUID => try std.json.encodeJsonString(&src.get(zuckdb.UUID, i), .{}, writer),
			zuckdb.c.DUCKDB_TYPE_DATE => {
				// std.fmt's integer formatting is broken when dealing with signed integers
				// we use our own formatter
				// https://github.com/ziglang/zig/issues/19488
				const date = src.get(zuckdb.Date, i);
				try std.fmt.format(writer, "\"{d}-{s}-{s}\"", .{date.year, paddingTwoDigits(date.month), paddingTwoDigits(date.day)});
			},
			zuckdb.c.DUCKDB_TYPE_TIME => {
				// std.fmt's integer formatting is broken when dealing with signed integers
				// we use our own formatter. But for micros, I'm lazy and cast it to unsigned,
				// which std.fmt handles better.
				const time = src.get(zuckdb.Time, i);
				try std.fmt.format(writer, "\"{s}:{s}:{s}.{d:6>0}\"", .{paddingTwoDigits(time.hour), paddingTwoDigits(time.min), paddingTwoDigits(time.sec), @as(u32, @intCast(time.micros))});
			},
			zuckdb.c.DUCKDB_TYPE_TIMESTAMP, zuckdb.c.DUCKDB_TYPE_TIMESTAMP_TZ  => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_INTERVAL => {
				const interval = src.get(zuckdb.Interval, i);
				try std.fmt.format(writer, "{{\"months\":{d},\"days\":{d},\"micros\":{d}}}", .{interval.months, interval.days, interval.micros});
			},
			zuckdb.c.DUCKDB_TYPE_BIT => try std.json.encodeJsonString(try zuckdb.bitToString(allocator, src.get([]u8, i)), .{}, writer),
			zuckdb.c.DUCKDB_TYPE_BLOB => {
				const v = src.get([]const u8, i);
				const encoder = std.base64.standard.Encoder;
				const out = try allocator.alloc(u8, encoder.calcSize(v.len));
				try std.json.encodeJsonString(encoder.encode(out, v), .{}, writer);
			},
			else => |duckdb_type| {
				try writer.writeAll("\"???\"");
				logger.level(.Warn).ctx("serialize.unknown_type").int("duckdb_type", duckdb_type).log();
			}
		},
	}
}

fn writeVarcharType(result: *zuckdb.c.duckdb_result, column_index: usize, buf: *zul.StringBuilder) !void {
	var logical_type = zuckdb.c.duckdb_column_logical_type(result, column_index);
	defer zuckdb.c.duckdb_destroy_logical_type(&logical_type);
	const alias = zuckdb.c.duckdb_logical_type_get_alias(logical_type);
	if (alias == null) {
		return buf.write("varchar");
	}
	defer zuckdb.c.duckdb_free(alias);
	return buf.write(std.mem.span(alias));
}

fn paddingTwoDigits(value: i8) [2]u8 {
	std.debug.assert(value < 61 and value > 0);
	const digits = "0001020304050607080910111213141516171819" ++
		"2021222324252627282930313233343536373839" ++
		"4041424344454647484950515253545556575859" ++
		"60";
	const index: usize = @intCast(value);
	return digits[index * 2 ..][0..2].*;
}

fn isSingleI64Result(rows: *const zuckdb.Rows) bool {
	if (rows.column_count != 1) {
		return false;
	}
	if (rows.count() != 1) {
		return false;
	}

	switch (rows.vectors[0].type) {
		.scalar => |s| switch (s) {
			.simple => |duckdb_type| return duckdb_type == zuckdb.c.DUCKDB_TYPE_BIGINT,
			else => return false,
		},
		else => return false,
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
test "exec: invalid json body" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.body("{hi");
	try t.expectError(error.InvalidJson, handler(tc.env, tc.web.req, tc.web.res));
}

test "exec: invalid input" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = 32, .params = true});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.TYPE_STRING, .field = "sql"});
	try tc.expectInvalid(.{.code = validate.codes.TYPE_ARRAY, .field = "params"});
}

test "exec: invalid sql" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "update x", });
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = dproxy.val.INVALID_SQL, .field = "sql", .err = "Parser Error: syntax error at end of input"});
}

test "exec: wrong parameters" {
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

test "exec: invalid parameter value" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select $1::bool", .params = .{"abc"}});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.TYPE_BOOL, .field = "params.0"});
}

test "exec: invalid base64 for blog" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "select $1::blob", .params = .{"not a blob"}});
	try t.expectError(error.Validation, handler(tc.env, tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = validate.codes.STRING_BASE64, .field = "params.0"});
}

test "exec: no changes" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "update everythings set col_integer = 1 where col_varchar = $1", .params = .{"does not exist"}});
	try handler(tc.env, tc.web.req, tc.web.res);
	try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{0}}});
}

test "exec: change with no result" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{.sql = "insert into everythings (col_varchar) values ($1)", .params = .{"insert no results"}});
	try handler(tc.env, tc.web.req, tc.web.res);
	try tc.web.expectJson(.{.cols = .{"Count"}, .rows = .{.{1}}});

	const row = tc.getRow("select count(*) as count from everythings where col_varchar = 'insert no results'", .{}).?;
	defer row.deinit();
	try t.expectEqual(1, row.get(i64, 0));
}

test "exec: every type" {
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
			"13:35:29.332000",
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
test "exec: interval as object" {
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

test "exec: returning multiple rows" {
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
test "exec: special count case" {
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

test "exec: with_wrap" {
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

test "exec: max_limit" {
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
