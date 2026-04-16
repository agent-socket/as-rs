default: build

build:
    cargo build

test:
    cargo test

clean:
    cargo clean

fmt:
    cargo fmt

check:
    cargo check --all-targets

echo:
    cargo run --example echo -- config.json

simple:
    cargo run --example simple -- config.json
