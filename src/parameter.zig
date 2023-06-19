const std = @import("std");
const typed = @import("typed");
const zuckdb = @import("zuckdb");
const validate = @import("validate");

const Allocator = std.mem.Allocator;

pub const Parameter = struct {
	tpe: Type,
	index: usize,
	value: typed.Value,

	pub fn init(index: usize, tpe: Type, value: typed.Value) Parameter {
		return .{
			.index = index,
			.tpe = tpe,
			.value = value,
		};
	}

	pub fn validateValue(self: *Parameter, aa: Allocator, validator: *validate.Context(void)) !void {
		const value = self.value;
		if (std.meta.activeTag(value) == typed.Value.null) {
			// null is always valid
			return;
		}

		validator.field = try validationField(aa, self.index);
		self.value = switch (self.tpe) {
			.bool => try bool_validator.validateValue(value, validator),
			.uuid => try uuid_validator.validateValue(value, validator),
			.i8 => try i8_validator.validateValue(value, validator),
			.i16 => try i16_validator.validateValue(value, validator),
			.i32 => try i32_validator.validateValue(value, validator),
			.i64 => try i64_validator.validateValue(value, validator),
			.i128 => try i128_validator.validateValue(value, validator),
			.u8 => try u8_validator.validateValue(value, validator),
			.u16 => try u16_validator.validateValue(value, validator),
			.u32 => try u32_validator.validateValue(value, validator),
			.u64 => try u64_validator.validateValue(value, validator),
			.f32 => try f32_validator.validateValue(value, validator),
			.f64 => try f64_validator.validateValue(value, validator),
			.decimal => try f64_validator.validateValue(value, validator),
			.timestamp => try i64_validator.validateValue(value, validator),
			.varchar => try string_validator.validateValue(value, validator),
			.blob => try string_validator.validateValue(value, validator),
			.date => try date_validator.validateValue(value, validator),
			.time => try time_validator.validateValue(value, validator),
		};
	}

	pub fn bind(self: Parameter, stmt: zuckdb.Stmt) !void {
		const index = self.index;
		const value = self.value;
		if (std.meta.activeTag(value) == typed.Value.null) {
			// null is always valid
			return stmt.bindDynamic(index, null);
		}

		switch (self.tpe) {
			.bool => return stmt.bindDynamic(index, value.get(bool).?),
			.uuid => return stmt.bindDynamic(index, value.get([]u8).?),
			.i8 => return stmt.bindDynamic(index, value.get(i8).?),
			.i16 => return stmt.bindDynamic(index, value.get(i16).?),
			.i32 => return stmt.bindDynamic(index, value.get(i32).?),
			.i64 => return stmt.bindDynamic(index, value.get(i64).?),
			.i128 => return stmt.bindDynamic(index, value.get(i128).?),
			.u8 => return stmt.bindDynamic(index, value.get(u8).?),
			.u16 => return stmt.bindDynamic(index, value.get(u16).?),
			.u32 => return stmt.bindDynamic(index, value.get(u32).?),
			.u64 => return stmt.bindDynamic(index, value.get(u64).?),
			.f32 => return stmt.bindDynamic(index, value.get(f32).?),
			.f64 => return stmt.bindDynamic(index, value.get(f64).?),
			.decimal => return stmt.bindDynamic(index, value.get(f64).?),
			.timestamp => return stmt.bindDynamic(index, value.get(i64).?),
			.varchar => return stmt.bindDynamic(index, value.get([]u8).?),
			.blob => return stmt.bindDynamic(index, value.get([]u8).?),
			.date => {
				const date = value.get(typed.Date).?;
				return stmt.bindDynamic(index, zuckdb.Date{
					.year = date.year,
					.month = @intCast(i8, date.month),
					.day = @intCast(i8, date.day),
				});
			},
			.time => {
				const time = value.get(typed.Time).?;
				return stmt.bindDynamic(index, zuckdb.Time{
					.hour = @intCast(i8, time.hour),
					.min = @intCast(i8, time.min),
					.sec = @intCast(i8, time.sec),
					.micros = @intCast(i32, time.micros),
				});
			},
		}
	}

	pub fn mapType(ztpe: zuckdb.ParameterType) ?Type {
		switch (ztpe) {
			.bool => return .bool,
			.uuid => return .uuid,
			.i8 => return .i8,
			.i16 => return .i16,
			.i32 => return .i32,
			.i64 => return .i64,
			.i128 => return .i128,
			.u8 => return .u8,
			.u16 => return .u16,
			.u32 => return .u32,
			.u64 => return .u64,
			.f32 => return .f32,
			.f64 => return .f64,
			.decimal => return .decimal,
			.timestamp => return .timestamp,
			.varchar => return .varchar,
			.blob => return .blob,
			.date => return .date,
			.time => return .time,
			.interval => return null,
			.@"enum" => return null, // https://github.com/duckdb/duckdb/discussions/7635
			.unknown => return null,  // can't support what the driver doesn't
		}
	}

	pub const Type = enum {
		bool,
		uuid,
		i8,
		i16,
		i32,
		i64,
		i128,
		u8,
		u16,
		u32,
		u64,
		f32,
		f64,
		decimal,
		timestamp,
		varchar,
		blob,
		date,
		time,
	};
};

// We want to give nice error messages that reference the exact parameter
// which is invalid. This is an unfortunate detail of our validation framework.
// Normally, it could take care of it internally, but we're manually validating
// the params, because it's dynamic, based on the specific query.
fn validationField(allocator: Allocator, i: usize) !validate.Field {
	return switch (i) {
		0 => .{.name = "", .path = "params.0", .parts = null},
		1 => .{.name = "", .path = "params.1", .parts = null},
		2 => .{.name = "", .path = "params.2", .parts = null},
		3 => .{.name = "", .path = "params.3", .parts = null},
		4 => .{.name = "", .path = "params.4", .parts = null},
		5 => .{.name = "", .path = "params.5", .parts = null},
		6 => .{.name = "", .path = "params.6", .parts = null},
		7 => .{.name = "", .path = "params.7", .parts = null},
		8 => .{.name = "", .path = "params.8", .parts = null},
		9 => .{.name = "", .path = "params.9", .parts = null},
		10 => .{.name = "", .path = "params.10", .parts = null},
		11 => .{.name = "", .path = "params.11", .parts = null},
		12 => .{.name = "", .path = "params.12", .parts = null},
		13 => .{.name = "", .path = "params.13", .parts = null},
		14 => .{.name = "", .path = "params.14", .parts = null},
		15 => .{.name = "", .path = "params.15", .parts = null},
		16 => .{.name = "", .path = "params.16", .parts = null},
		17 => .{.name = "", .path = "params.17", .parts = null},
		18 => .{.name = "", .path = "params.18", .parts = null},
		19 => .{.name = "", .path = "params.19", .parts = null},
		20 => .{.name = "", .path = "params.20", .parts = null},
		21 => .{.name = "", .path = "params.21", .parts = null},
		22 => .{.name = "", .path = "params.22", .parts = null},
		23 => .{.name = "", .path = "params.23", .parts = null},
		24 => .{.name = "", .path = "params.24", .parts = null},
		25 => .{.name = "", .path = "params.25", .parts = null},
		26 => .{.name = "", .path = "params.26", .parts = null},
		27 => .{.name = "", .path = "params.27", .parts = null},
		28 => .{.name = "", .path = "params.28", .parts = null},
		29 => .{.name = "", .path = "params.29", .parts = null},
		30 => .{.name = "", .path = "params.30", .parts = null},
		31 => .{.name = "", .path = "params.31", .parts = null},
		32 => .{.name = "", .path = "params.32", .parts = null},
		33 => .{.name = "", .path = "params.33", .parts = null},
		34 => .{.name = "", .path = "params.34", .parts = null},
		35 => .{.name = "", .path = "params.35", .parts = null},
		36 => .{.name = "", .path = "params.36", .parts = null},
		37 => .{.name = "", .path = "params.37", .parts = null},
		38 => .{.name = "", .path = "params.38", .parts = null},
		39 => .{.name = "", .path = "params.39", .parts = null},
		40 => .{.name = "", .path = "params.40", .parts = null},
		41 => .{.name = "", .path = "params.41", .parts = null},
		42 => .{.name = "", .path = "params.42", .parts = null},
		43 => .{.name = "", .path = "params.43", .parts = null},
		44 => .{.name = "", .path = "params.44", .parts = null},
		45 => .{.name = "", .path = "params.45", .parts = null},
		46 => .{.name = "", .path = "params.46", .parts = null},
		47 => .{.name = "", .path = "params.47", .parts = null},
		48 => .{.name = "", .path = "params.48", .parts = null},
		49 => .{.name = "", .path = "params.49", .parts = null},
		50 => .{.name = "", .path = "params.50", .parts = null},
		else => .{.name = "", .path = try std.fmt.allocPrint(allocator, "params.{d}", .{i})}
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

// Called in init.zig
pub fn init(builder: *validate.Builder(void)) !void {
	// All of these validators are very simple. They largely just assert type
	// correctness.
	i8_validator = builder.int(i8, .{});
	i16_validator = builder.int(i16, .{});
	i32_validator = builder.int(i32, .{});
	i64_validator = builder.int(i64, .{});
	i128_validator = builder.int(i128, .{});
	u8_validator = builder.int(u8, .{});
	u16_validator = builder.int(u16, .{});
	u32_validator = builder.int(u32, .{});
	u64_validator = builder.int(u64, .{});
	f32_validator = builder.float(f32, .{});
	f64_validator = builder.float(f64, .{});
	bool_validator = builder.boolean(.{});
	uuid_validator = builder.uuid(.{});
	date_validator = builder.date(.{.parse = true});
	time_validator = builder.time(.{.parse = true});
	string_validator = builder.string(.{});
}
