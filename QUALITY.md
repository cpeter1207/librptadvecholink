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

The complete descriptor exposes account, network, media, text, and observation
operations. Lifecycle-only sized prefixes remain usable; consumers that need
network service require the complete operation group. The final PR gate must
cover the current revision, including its packet and disconnect fixes.

Fixture and native quality checks do not establish public-network acceptance.
Check incoming and outgoing calls, both audio directions, silence, text,
disconnect, and reload with the clients, controller host, and direct or Proxy
transport that will be used. No package release is implied by a passing gate.
