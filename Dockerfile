FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY . .

ARG FEATURES=http-api,opentelemetry
RUN cargo build --profile dist --package mqdb-cli --no-default-features --features ${FEATURES}

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/target/dist/mqdb /mqdb

EXPOSE 1883 8080 3000

ENTRYPOINT ["/mqdb"]
CMD ["--help"]
