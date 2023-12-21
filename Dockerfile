FROM rust:bookworm as builder
WORKDIR /usr/src/target-iceberg-sql
COPY ./target-iceberg-sql/ .
RUN cargo install --path .
FROM python:3.11-slim-bookworm
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/target-iceberg-sql /usr/local/bin/target-iceberg-sql
CMD ["target-iceberg-sql"]
