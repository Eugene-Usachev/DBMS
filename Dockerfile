#FROM rust:latest as build
#
#WORKDIR /test-tcp
#
#COPY ./Cargo.lock ./Cargo.lock
#COPY ./Cargo.toml ./Cargo.toml
#
#COPY ./src ./src
#
## Build for release.
#RUN cargo build --release
#
#FROM debian:buster-slim
#
#COPY --from=build /test-tcp/target/release/db /usr/src/db
#
#EXPOSE 8081
#
#CMD ["/usr/src/db"]

FROM rust:latest as build

WORKDIR /test-tcp

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

COPY ./src ./src

EXPOSE 8081

RUN cargo build --release

CMD ["/test-tcp/target/release/db"]