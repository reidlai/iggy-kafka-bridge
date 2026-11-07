# Multi-stage Dockerfile for iggy-kafka-bridge
# Non-root user: 10001:10001

FROM rust:1.99-alpine AS builder

RUN apk add --no-cache musl-dev gcc make

WORKDIR /usr/src/iggy-kafka-bridge

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release --locked

FROM alpine:3.20

RUN addgroup -g 10001 -S bridge && \
    adduser -u 10001 -S bridge -G bridge

USER 10001:10001
WORKDIR /app

COPY --from=builder /usr/src/iggy-kafka-bridge/target/release/iggy-kafka-bridge /app/iggy-kafka-bridge

EXPOSE 9092

ENTRYPOINT ["/app/iggy-kafka-bridge"]
CMD ["--config", "/contracts/topics.yml"]
