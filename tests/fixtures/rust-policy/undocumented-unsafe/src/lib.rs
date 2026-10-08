// Seeded violation for scripts/ci/rust-policy.sh: unsafe block without a SAFETY comment
// (REQ-0048 AC2). Not compiled; this directory is not a Cargo package.

pub fn read_first(xs: &[u8]) -> u8 {
    // Deliberately missing the required comment.
    unsafe { *xs.as_ptr() }
}
