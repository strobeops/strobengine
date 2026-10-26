//! Build script: compile the distributed control-plane `.proto` into Rust.
//!
//! Uses the pure-Rust `protox` compiler so no system `protoc` is required, then
//! hands the resulting `FileDescriptorSet` to `tonic-prost-build`.

fn add_debug_libpython_rpath() {
    // pyo3 links libpython into debug builds but only adds an rpath on macOS.
    // Cargo has no per-test link argument for the generated lib test harness, so
    // apply the rpath to debug builds (which includes `cargo test`); release
    // wheels and binaries stay clean.
    #[cfg(target_family = "unix")]
    {
        let profile = std::env::var("PROFILE").unwrap_or_default();
        if profile != "debug" && profile != "test" {
            return;
        }
        if std::env::var("HOST").ok() == std::env::var("TARGET").ok()
            && let Some(lib_dir) = pyo3_build_config::get().lib_dir()
        {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/loadtesting/v1/load_test_controller.proto");

    add_debug_libpython_rpath();

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
