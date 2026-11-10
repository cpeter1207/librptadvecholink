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

Alpha2 appends a complete optional egress pair to the released ABI1 descriptor;
the released prefix, capabilities and SONAME1 remain compatible. Consumers must
check the descriptor's physical `struct_size` before copying either tail slot.
On the existing serial QSO owner, retirement revokes old voice and resets its
actual encoder. `Pending` requires retry; only `Ok` with a nonzero epoch certifies
that no old voice remains eligible in the native queue. New PCM and voice sends
remain gated while retirement is pending. Other QSOs, control and chat survive.

The metadata-only observer can outlive QSO/account/context destruction without
retaining those I/O owners. Destruction alone is insufficient: an already
partially submitted Proxy TCP frame must finish or lose its actual transport
before the observer reports disposal. Unsubmitted retired voice is discarded.
Keep the provider DSO loaded until releasing the observer. Neither completion nor
UDP/TCP acceptance proves remote reception or RF silence. The observer and queue
tests exercise actual bounded writer ownership, codec reset failures and native
teardown; they do not establish public Proxy or radio interoperability.

Fixture and native quality checks do not establish public-network acceptance.
Check incoming and outgoing calls, both audio directions, silence, text,
disconnect, and reload with the clients, controller host, and direct or Proxy
transport that will be used. No package release is implied by a passing gate.
