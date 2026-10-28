# Quality checks

The initial packet-library fit candidate has no production DSO yet. Run
`cargo +1.85.0 fmt --all -- --check`,
`cargo +1.85.0 clippy --locked --all-targets -- -D warnings`, and
`cargo +1.85.0 test --locked --test packet_library_fit`.

The thin hosted caller runs these focused checks in the maintained Debian 13
amd64 quality image. It freshly pulls and records the digest through the shared
labeled launcher, with two CPUs, 4 GiB RAM and 256 PIDs, then cleans its container.
This is not the full production quality gate or live interoperability evidence.
Task 2 of the approved plan adds C-ABI, documentation, production coverage and
native amd64/arm64 package/install gates before production implementation.
