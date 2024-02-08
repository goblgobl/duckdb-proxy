.PHONY: t
t:
	zig build test --summary all -freference-trace

.PHONY: s
s:
	zig build run -freference-trace

.PHONY: release
release:
	# Can't use buid.zig until this is fixed:
	# https://github.com/ziglang/zig/issues/15849
	zig build-exe \
		--name duckdb-proxy \
		-O ReleaseFast \
		--cache-dir zig-cache \
		--global-cache-dir ~/.cache/zig \
		--dep zul --dep logz --dep typed --dep httpz --dep zuckdb --dep  validate \
		-Mroot=src/main.zig \
		-Mzul=lib/zul/src/zul.zig \
		-Mlogz=lib/log.zig/src/logz.zig \
		-Mtyped=lib/typed.zig/src/typed.zig \
		--dep typed -Mvalidate=lib/validate.zig/src/validate.zig \
		-Mwebsocket=lib/websocket.zig/src/websocket.zig \
		--dep websocket -Mhttpz=lib/http.zig/src/httpz.zig \
		-Mzuckdb=lib/zuckdb.zig/src/zuckdb.zig \
		-I lib/duckdb/ \
		-I lib/zuckdb.zig/lib/ \
		-L lib/duckdb/ \
		-lduckdb \
		-rpath . \
		-target $(TARGET) \
		$(ARGS)

	mkdir -p release/duckdb-proxy-$(TARGET)
