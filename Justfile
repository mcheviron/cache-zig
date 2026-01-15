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

std:
    zig std
