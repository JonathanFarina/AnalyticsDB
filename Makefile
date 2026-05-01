.PHONY: build test test-sql-cli fmt lint web-admin-install web-admin-test web-admin-build

build:
	. "$$HOME/.cargo/env" && cargo build --workspace

test:
	. "$$HOME/.cargo/env" && cargo test --workspace

test-sql-cli:
	. "$$HOME/.cargo/env" && cargo test -p analyticsdb-cli --test sql_cli

fmt:
	. "$$HOME/.cargo/env" && cargo fmt --all

lint:
	. "$$HOME/.cargo/env" && cargo clippy --workspace --all-targets -- -D warnings

web-admin-install:
	cd web/admin-console && npm install

web-admin-test:
	cd web/admin-console && npm test

web-admin-build:
	cd web/admin-console && npm run build
