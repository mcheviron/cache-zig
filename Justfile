set positional-arguments

help:
    just -l

install:
    zig version

fmt:
    zig fmt build.zig src

build *args:
    zig build "$@"

test *args:
    zig build test "$@"

test-one filter *args:
    zig test src/root.zig --test-filter "$filter" "$@"

bench *args:
    zig build -Doptimize=ReleaseFast bench "$@"

bench-synth *args:
    zig build -Doptimize=ReleaseFast bench-synth "$@"

bench-trace *args:
    zig build -Doptimize=ReleaseFast bench-trace "$@"

bench-compare *args:
    zig build -Doptimize=ReleaseFast bench-compare "$@"

std:
    zig std
