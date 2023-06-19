const logz = @import("logz");
const validate = @import("validate");
const dproxy = @import("dproxy.zig");

pub const Env = struct {
	app: *dproxy.App,

	// This logger has the "$rid=REQUEST_ID" attribute automatically added to any
	// generated log.
	logger: logz.Logger,

	// Most request will do some validation, so we load a validation context with
	// every request. Makes it its lifecycle can be managed by the dispatcher.
	validator: *validate.Context(void)
};
