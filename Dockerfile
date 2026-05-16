# Multi-stage Dockerfile for AnalyticsDB server
# Stage 1: Build
FROM rust:1.75-bookworm as builder

WORKDIR /usr/src/analyticsdb

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/analyticsdb-core/Cargo.toml ./crates/analyticsdb-core/
COPY crates/analyticsdb-control/Cargo.toml ./crates/analyticsdb-control/
COPY crates/analyticsdb-engine/Cargo.toml ./crates/analyticsdb-engine/
COPY crates/analyticsdb-protocol/Cargo.toml ./crates/analyticsdb-protocol/
COPY crates/analyticsdb-server/Cargo.toml ./crates/analyticsdb-server/
COPY crates/analyticsdb-cli/Cargo.toml ./crates/analyticsdb-cli/

# Create dummy files to satisfy build (if needed)
RUN mkdir -p crates/analyticsdb-core/src && echo "pub fn dummy() {}" > crates/analyticsdb-core/src/lib.rs
RUN mkdir -p crates/analyticsdb-control/src && echo "pub fn dummy() {}" > crates/analyticsdb-control/src/lib.rs
RUN mkdir -p crates/analyticsdb-engine/src && echo "pub fn dummy() {}" > crates/analyticsdb-engine/src/lib.rs
RUN mkdir -p crates/analyticsdb-protocol/src && echo "pub fn dummy() {}" > crates/analyticsdb-protocol/src/lib.rs
RUN mkdir -p crates/analyticsdb-server/src && echo "fn main() {}" > crates/analyticsdb-server/src/main.rs
RUN mkdir -p crates/analyticsdb-cli/src && echo "fn main() {}" > crates/analyticsdb-cli/src/main.rs

# Build dependencies (this will cache)
RUN cargo build --release --package analyticsdb-server 2>&1 || true

# Now copy actual source
COPY . .

# Build actual binary
RUN cargo build --release --package analyticsdb-server

# Stage 2: Minimal runtime image
FROM gcr.io/distroless/cc-debian12 as runtime

COPY --from=builder /usr/src/analyticsdb/target/release/analyticsdb-server /usr/local/bin/analyticsdb-server

# Create non-root user
RUN groupadd -r analyticsdb && useradd -r -g analyticsdb analyticsdb || true

USER analyticsdb:analyticsdb

EXPOSE 5432 8815 8816

ENTRYPOINT ["/usr/local/bin/analyticsdb-server"]
