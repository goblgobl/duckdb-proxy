.PHONY: t
t:
	zig build test -fsummary -freference-trace

.PHONY: s
s:
	zig build run -freference-trace
