# Packet-library fit decision

The provider uses released `rtp`/`rtcp` 0.12.0 and
`webrtc-util` 0.10.0 with its `marshal` feature. Select their MIT license option
for this GPL-2.0-only project. These are implementation crates inside the owned
provider, not a public ABI or the full WebRTC stack.

Observed on Rust 1.85.0:

- 0.17.2 cannot build: its required util calls unstable `usize::is_multiple_of`.
  The 0.14.0 release source has the same call. No fork or toolchain upgrade is used.
- 0.12.0 builds and preserves RTP version 3, zero clocks/SSRC, wrapping sequence,
  and EchoLib's marker/PT interpretation. It preserves opaque raw PRIV `SPEEX`.
- The smaller `rtcp-types` 0.3.0 candidate was tested and rejects raw `SPEEX`
  because it enforces standard PRIV prefix encoding. Comparison-only dependencies
  and their temporary characterization test were removed after recording that result.
- RTCP uses a narrow version-bit wrapper, EchoLib surplus-zero-word handling,
  and ASL-compatible END alignment. Only the immediate END alignment word may
  contain nonzero bytes; item bounds, an actual END, identity, outer padding,
  and surplus-word validation remain enforced. Typed library fields supply
  parsing and serialization. Outgoing SDES alignment is zero-filled after
  serialization to match the reference maker.

Fixture tests use independently source-derived literal bytes, not captured
traffic; see `tests/fixtures/README.md`. They are not live interoperability proof.
The complete wire tests also cover optional items, bounds, and END alignment.
Dependency feature/license evidence is kept in ignored `.work/`. The selected
library maps unknown SDES types to END: the wire wrapper checks their bounds
and narrowly normalizes them before parsing. Their contents are not treated
as a station field or human text.

The packet dependencies do not add WebRTC, ICE, DTLS, SRTP, SDP, or SCTP.
Codec and account/network work stay outside the wire wrapper. Tokio is locked
to 1.48.0; no runtime is started by the packet-library fit tests. Broader library
upgrades need the same MSRV, license and independent wire-fixture gate.
