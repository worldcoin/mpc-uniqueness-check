### Base stage
FROM debian:12 as base

WORKDIR /src

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
    curl build-essential \
    libssl-dev texinfo \
    libcap2-bin pkg-config

# TODO: Use a specific version of rustup
# Install rustup
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

COPY ./rust-toolchain.toml ./rust-toolchain.toml

# Set environment variables
ENV PATH="/root/.cargo/bin:${PATH}"
ENV RUSTUP_HOME="/root/.rustup"
ENV CARGO_HOME="/root/.cargo"

# Install the toolchain
RUN rustup component add cargo

# Install cargo chef
RUN cargo install cargo-chef --locked

### Recipe cooking stage
FROM base as build-env-base

# Copy everything
COPY . .

# Prepare the recipe
RUN cargo chef prepare --recipe-path recipe.json

### Build stage
FROM base as build-env

# Copy recipe
COPY --from=build-env-base /src/recipe.json ./recipe.json

# Build the dependencies
RUN cargo chef cook --release --recipe-path ./recipe.json

# Copy the remaining source code
COPY . .

ARG BIN=mpc-node

# Build the binary
RUN cargo build --release --bin $BIN --no-default-features

# Make sure it runs
RUN /src/target/release/$BIN --version

### Runtime stage
# cc variant because we need libgcc and others
FROM gcr.io/distroless/cc-debian12:nonroot

ARG BIN=mpc-node

# Copy the binary
# This is ok when building but when running fails with:
# `docker: Error response from daemon: failed to create task for container: failed to create shim task: OCI runtime create failed: runc create failed: unable to start container process: exec: "/bin/mpc": is a directory: unknown: permission denied.`
COPY --from=build-env --chown=0:10001 --chmod=010 /src/target/release/$BIN /bin/mpc

ENTRYPOINT [ "/bin/mpc" ]
