# Multi-stage Dockerfile for iggy-kafka-proxy
# Non-root user: 10001:10001

FROM rust:1.99-alpine AS builder

RUN apk add --no-cache musl-dev gcc make

WORKDIR /usr/src/iggy-kafka-proxy

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release --locked

FROM alpine:3.20

RUN addgroup -g 10001 -S proxy && \
    adduser -u 10001 -S proxy -G proxy

USER 10001:10001
WORKDIR /app

COPY --from=builder /usr/src/iggy-kafka-proxy/target/release/iggy-kafka-proxy /app/iggy-kafka-proxy
RUN ln -s /app/iggy-kafka-proxy /app/iggy-kafka-bridge

EXPOSE 9092 9093

ENTRYPOINT ["/app/iggy-kafka-proxy"]
CMD ["--config", "/contracts/topics.yml"]
