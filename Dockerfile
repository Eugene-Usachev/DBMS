ARG RUST_VERSION=rust:1.75.0-slim-bullseye
ARG APP_NAME=dbms
FROM ${RUST_VERSION} AS build
ARG APP_NAME
WORKDIR /app

RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    <<EOF
set -e
cargo build --locked --release
cp ./target/release/$APP_NAME /bin/server
EOF


FROM debian:bullseye-slim AS final

USER root

COPY --from=build /bin/server /bin/

EXPOSE 8082

CMD ["/bin/server"]
