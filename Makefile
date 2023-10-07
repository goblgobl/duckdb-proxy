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
	zig build-exe src/main.zig \
		--name duckdb-proxy \
		-O ReleaseFast \
		--cache-dir zig-cache \
		--global-cache-dir ~/.cache/zig \
		--mod uuid::lib/uuid/uuid.zig \
		--mod logz::lib/log.zig/src/logz.zig \
		--mod typed::lib/typed.zig/src/typed.zig \
		--mod yazap::lib/yazap/src/lib.zig \
		--mod httpz::lib/http.zig/src/httpz.zig \
		--mod zuckdb::lib/zuckdb.zig/src/zuckdb.zig \
		--mod validate:typed:lib/validate.zig/src/validate.zig \
		--mod buffer:typed:lib/buffer.zig/src/buffer.zig \
		--deps uuid,logz,typed,yazap,httpz,zuckdb,validate,buffer \
		-I lib/duckdb/ \
		-I lib/zuckdb.zig/lib/ \
		-L lib/duckdb/ \
		-lduckdb \
		-rpath . \
		-target $(TARGET) \
		$(ARGS)

	mkdir -p release/duckdb-proxy-$(TARGET)
