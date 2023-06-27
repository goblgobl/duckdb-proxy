const std = @import("std");
const typed = @import("typed");
const zuckdb = @import("zuckdb");
const validate = @import("validate");
const dproxy = @import("dproxy.zig");

const Allocator = std.mem.Allocator;

pub const Parameter = struct {
	// This does a lot, but having validation + binding in a single place does
	// streamline a lot of code.
	pub fn validateAndBind(aa: Allocator, index: usize, stmt: zuckdb.Stmt, value: typed.Value, validator: *validate.Context(void)) !void {
		if (std.meta.activeTag(value) == typed.Value.null) {
			return stmt.bindDynamic(index, null);
		}

		// for the "field" of the error message
		validator.field = null;
		validator.force_prefix = try fieldName(aa, index);
		switch (stmt.parameterType(index)) {
			.bool => {
				switch (try bool_validator.validateValue(value, validator)) {
					.bool => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.uuid => {
				switch (try uuid_validator.validateValue(value, validator)) {
					.string => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.i8 => {
				switch (try i8_validator.validateValue(value, validator)) {
					.i8 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.i16 => {
				switch (try i16_validator.validateValue(value, validator)) {
					.i16 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.i32 => {
				switch (try i32_validator.validateValue(value, validator)) {
					.i32 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.i64 => {
				switch (try i64_validator.validateValue(value, validator)) {
					.i64 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.i128 => {
				switch (try i128_validator.validateValue(value, validator)) {
					.i128 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.u8 => {
				switch (try u8_validator.validateValue(value, validator)) {
					.u8 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.u16 => {
				switch (try u16_validator.validateValue(value, validator)) {
					.u16 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.u32 => {
				switch (try u32_validator.validateValue(value, validator)) {
					.u32 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.u64 => {
				switch (try u64_validator.validateValue(value, validator)) {
					.u64 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.f32 => {
				switch (try f32_validator.validateValue(value, validator)) {
					.f32 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.f64 => {
				switch (try f64_validator.validateValue(value, validator)) {
					.f64 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.decimal => {
				switch (try f64_validator.validateValue(value, validator)) {
					.f64 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.timestamp => {
				switch (try i64_validator.validateValue(value, validator)) {
					.i64 => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.varchar => {
				switch (try string_validator.validateValue(value, validator)) {
					.string => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.blob => {
				switch (try blob_validator.validateValue(value, validator)) {
					.string => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.bitstring => {
				switch (try bitstring_validator.validateValue(value, validator)) {
					.string => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.date => {
				switch (try date_validator.validateValue(value, validator)) {
					.date => |v| {
						return stmt.bindDynamic(index, zuckdb.Date{
							.year = v.year,
							.month = @intCast(v.month),
							.day = @intCast(v.day),
						});
					},
					else => return stmt.bindDynamic(index, null),
				}
			},
			.time => {
				switch (try time_validator.validateValue(value, validator)) {
					.time => |v| {
						return stmt.bindDynamic(index, zuckdb.Time{
							.hour = @intCast(v.hour),
							.min = @intCast(v.min),
							.sec = @intCast(v.sec),
							.micros = @intCast(v.micros),
						});
					},
					else => return stmt.bindDynamic(index, null),
				}
			},
			.@"enum" => {
				switch (try string_validator.validateValue(value, validator)) {
					.string => |v| return stmt.bindDynamic(index, v),
					else => return stmt.bindDynamic(index, null),
				}
			},
			.interval => {
				switch (value) {
					.string => {  // can either be a string, e.g. "4 hours"
						switch (try string_validator.validateValue(value, validator)) {
							.string => |v| return stmt.bindDynamic(index, v),
							else => return stmt.bindDynamic(index, null),
						}
					},
					else => { // or an object {months: X, days: Y, micros: Z}
						switch (try interval_validator.validateValue(value, validator)) {
							.map => |v| return stmt.bindDynamic(index, zuckdb.Interval{
								.months = v.get(i32, "months") orelse unreachable,
								.days = v.get(i32, "days") orelse unreachable,
								.micros = v.get(i64, "micros") orelse unreachable,
							}),
							else => return stmt.bindDynamic(index, null),
						}
					}
				}
			},
			else => |tpe| {
				const type_name = @tagName(tpe);
				return validator.add(.{
					.code = dproxy.val.UNSUPPORTED_PARAMETER_TYPE,
					.err = try std.fmt.allocPrint(aa, "Unsupported parameter type: ${d} - ${s}", .{index+1, type_name}),
					.data = try validator.dataBuilder().put("index", index).put("type", type_name).done(),
				});
			}
		}
	}

	// A few places need to generate this error, having it here makes sure that it's consistent
	pub fn invalidParameterCount(aa: Allocator, stmt_count: usize, input_count: usize, validator: *validate.Context(void)) !void {
		const err_format = "SQL statement requires {d} parameter{s}, {d} {s} given";
		const err = try std.fmt.allocPrint(aa, err_format, .{
			stmt_count,
			if (stmt_count == 1) "" else "s",
			input_count,
			if (input_count == 1) "was" else "were",
		});

		validator.addInvalidField(.{
			.err = err,
			.field = "params",
			.code = dproxy.val.WRONG_PARAMETER_COUNT,
			.data = try validator.dataBuilder().put("stmt", stmt_count).put("input", input_count).done(),
		});
		return error.Validation;
	}
};

// We want to give nice error messages that reference the exact parameter
// which is invalid. This is an unfortunate detail of our validation framework.
// Normally, it could take care of it internally, but we're manually validating
// the params, because it's dynamic, based on the specific query.
fn fieldName(allocator: Allocator, i: usize) ![]const u8 {
	return switch (i) {
		0 => "params.0",
		1 => "params.1",
		2 => "params.2",
		3 => "params.3",
		4 => "params.4",
		5 => "params.5",
		6 => "params.6",
		7 => "params.7",
		8 => "params.8",
		9 => "params.9",
		10 => "params.10",
		11 => "params.11",
		12 => "params.12",
		13 => "params.13",
		14 => "params.14",
		15 => "params.15",
		16 => "params.16",
		17 => "params.17",
		18 => "params.18",
		19 => "params.19",
		20 => "params.20",
		21 => "params.21",
		22 => "params.22",
		23 => "params.23",
		24 => "params.24",
		25 => "params.25",
		26 => "params.26",
		27 => "params.27",
		28 => "params.28",
		29 => "params.29",
		30 => "params.30",
		31 => "params.31",
		32 => "params.32",
		33 => "params.33",
		34 => "params.34",
		35 => "params.35",
		36 => "params.36",
		37 => "params.37",
		38 => "params.38",
		39 => "params.39",
		40 => "params.40",
		41 => "params.41",
		42 => "params.42",
		43 => "params.43",
		44 => "params.44",
		45 => "params.45",
		46 => "params.46",
		47 => "params.47",
		48 => "params.48",
		49 => "params.49",
		50 => "params.50",
		else => std.fmt.allocPrint(allocator, "params.{d}", .{i}),
	};
}

var i8_validator: *validate.Int(i8, void) = undefined;
var i16_validator: *validate.Int(i16, void) = undefined;
var i32_validator: *validate.Int(i32, void) = undefined;
var i64_validator: *validate.Int(i64, void) = undefined;
var i128_validator: *validate.Int(i128, void) = undefined;
var u8_validator: *validate.Int(u8, void) = undefined;
var u16_validator: *validate.Int(u16, void) = undefined;
var u32_validator: *validate.Int(u32, void) = undefined;
var u64_validator: *validate.Int(u64, void) = undefined;
var f32_validator: *validate.Float(f32, void) = undefined;
var f64_validator: *validate.Float(f64, void) = undefined;
var bool_validator: *validate.Bool(void) = undefined;
var uuid_validator: *validate.UUID(void) = undefined;
var date_validator: *validate.Date(void) = undefined;
var time_validator: *validate.Time(void) = undefined;
var string_validator: *validate.String(void) = undefined;
var blob_validator: *validate.String(void) = undefined;
var bitstring_validator: *validate.String(void) = undefined;
var interval_validator:  *validate.Object(void) = undefined;

// Called in init.zig
pub fn init(builder: *validate.Builder(void)) !void {
	// All of these validators are very simple. They largely just assert type
	// correctness.

	// std.json represents large integers are strings (fail), so we need to enable
	// test parsing for those.
	i8_validator = builder.int(i8, .{});
	i16_validator = builder.int(i16, .{});
	i32_validator = builder.int(i32, .{});
	i64_validator = builder.int(i64, .{.parse = true});
	i128_validator = builder.int(i128, .{.parse = true});
	u8_validator = builder.int(u8, .{});
	u16_validator = builder.int(u16, .{});
	u32_validator = builder.int(u32, .{});
	u64_validator = builder.int(u64, .{.parse = true});
	f32_validator = builder.float(f32, .{});
	f64_validator = builder.float(f64, .{});
	bool_validator = builder.boolean(.{});
	uuid_validator = builder.uuid(.{});
	date_validator = builder.date(.{.parse = true});
	time_validator = builder.time(.{.parse = true});
	string_validator = builder.string(.{});
	blob_validator = builder.string(.{.decode = .base64});
	bitstring_validator = builder.string(.{.function = validateBitstring});
	interval_validator = builder.object(&.{
		builder.field("months", builder.int(i32, .{.default = 0})),
		builder.field("days", builder.int(i32, .{.default = 0})),
		builder.field("micros", builder.int(i64, .{.default = 0})),
	}, .{});
}

fn validateBitstring(optional_value: ?[]const u8, context: *validate.Context(void)) !?[]const u8 {
	const value = optional_value orelse return null;
	for (value) |b| {
		if (b != '0' and b != '1') {
			try context.add(.{
				.code = dproxy.val.INVALID_BITSTRING,
				.err = "bitstring must contain only 0s and 1s",
			});
			return null;
		}
	}
	return value;

}
