# Quality checks

The packet-library fit check runs
`cargo +1.85.0 fmt --all -- --check`,
`cargo +1.85.0 clippy --locked --all-targets -- -D warnings`, and
`cargo +1.85.0 test --locked --test packet_library_fit`.

The thin hosted caller runs these focused checks in the maintained Debian 13
amd64 quality image. It freshly pulls and records the digest through the shared
labeled launcher, with two CPUs, 4 GiB RAM and 256 PIDs, then cleans its container.
This is not live interoperability evidence.

`make quality` runs formatting, Ruff, strict Clippy, Cppcheck, Doxygen and Rustdoc.
`make test` runs Rust tests and the C lifecycle smoke program. `make coverage`
requires all production Rust lines/branches on Debian 13 amd64; tests are excluded.
`make platform-verify` adds staged installation and Debian package checks. The
shared full PR gate runs native Debian 13 amd64 and arm64. Pushes run only
format/lint/static checks, and releases validate previously gated main revisions.

The initial descriptor advertises lifecycle only and rejects enabled accounts;
network/media operations are not claimed until their planned implementations
and focused tests exist. No package from this branch is released or installed.
