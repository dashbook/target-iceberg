FROM rust:bookworm as builder
WORKDIR /usr/src/
COPY . .
RUN cargo install --path ./target-iceberg-dashbook
FROM python:3.11-slim-bookworm
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/target-iceberg-dashbook /usr/local/bin/target-iceberg-dashbook
CMD ["target-iceberg-dashbook"]
