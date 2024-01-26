FROM debian:12 as build-env

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

COPY . .

# Set environment variables
ENV PATH="/root/.cargo/bin:${PATH}"
ENV RUSTUP_HOME="/root/.rustup"
ENV CARGO_HOME="/root/.cargo"

# Install the toolchain
RUN rustup component add cargo

# Build the binary
RUN cargo fetch
RUN cargo build --release --no-default-features

# Make sure it runs
RUN /src/target/release/mpc-node --version

# cc variant because we need libgcc and others
FROM gcr.io/distroless/cc-debian12:nonroot

# Copy the mpc-node binary
COPY --from=build-env --chown=0:10001 --chmod=010 /src/target/release/mpc-node /bin/mpc-node

ENTRYPOINT [ "/bin/mpc-node" ]
