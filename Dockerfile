FROM rust:1.67

RUN git clone --depth=1 https://github.com/openmina/snark-coordinator-rs.git
WORKDIR "/snark-coordinator-rs"
RUN cargo build --release

EXPOSE 8080
ENTRYPOINT [ "./target/release/snark-coordinator-rs" ]
