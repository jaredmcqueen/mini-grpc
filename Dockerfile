FROM rust:latest as builder

# install protobuf
RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*

# copy over files
COPY Cargo.toml build.rs /usr/src/app/
COPY src /usr/src/app/src/
COPY proto /usr/src/app/proto/
WORKDIR /usr/src/app
RUN cargo build --bin server --release

# FROM gcr.io/distroless/static-debian11 as runner
FROM debian:bookworm-slim as runner
RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*
# get binary
COPY --from=builder /usr/src/app/target/release/server /

# set run env
EXPOSE 10000

# run it
CMD ["/server"]
