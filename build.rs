//! Build script: compile the distributed control-plane `.proto` into Rust.
//!
//! Uses the pure-Rust `protox` compiler so no system `protoc` is required, then
//! hands the resulting `FileDescriptorSet` to `tonic-prost-build`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/loadtesting/v1/load_test_controller.proto");

    let file_descriptor_set = protox::compile(
        ["proto/loadtesting/v1/load_test_controller.proto"],
        ["proto"],
    )?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(file_descriptor_set)?;

    Ok(())
}
