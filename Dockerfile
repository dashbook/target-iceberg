FROM rust:bookworm as builder
WORKDIR /usr/src
COPY . .
RUN cargo install --path ./target-iceberg-sql
FROM python:3.10-slim-bookworm
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/target-iceberg-sql /usr/local/bin/target-iceberg-sql
CMD ["target-iceberg-sql"]
