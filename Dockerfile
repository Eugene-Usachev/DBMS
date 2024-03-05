ARG APP_NAME=dbms

FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.3.0 AS xx

FROM --platform=$BUILDPLATFORM rustlang/rust:nightly-bullseye-slim AS build
ARG APP_NAME
WORKDIR /app

COPY --from=xx / /

RUN apt-get update && apt-get install -y clang lld musl-dev git file gcc

ARG TARGETPLATFORM

RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/app/target/,id=rust-cache-${APP_NAME}-${TARGETPLATFORM} \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    <<EOF
set -e
xx-cargo build --locked --release --target-dir ./target
cp ./target/$(xx-cargo --print-target-triple)/release/$APP_NAME /bin/app
xx-verify /bin/app
EOF

FROM debian:bullseye-slim AS final

USER root

COPY --from=build /bin/app /bin/

EXPOSE 8081

CMD ["/bin/app"]
