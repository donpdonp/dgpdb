.PHONY: all test
	
all:
	cargo build

format:
	find src -type f -exec rustfmt {} \;

test:
	cargo test
