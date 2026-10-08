fn main() {
    // A dylib request is recorded by cargo but not resolved when building an rlib,
    // so the fixture builds without the library existing.
    println!("cargo:rustc-link-lib=dylib=unlisted_native");
}
