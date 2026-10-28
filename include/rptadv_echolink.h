/* SPDX-License-Identifier: GPL-2.0-only */
/** @file
 * @brief Sized EchoLink provider lifecycle and serial-owner ABI.
 * All operations run off audio. Caller buffers are borrowed only during the call.
 * Stop and quiesce all callbacks/handles before unloading the provider DSO.
 */
#ifndef RPTADV_ECHOLINK_H
#define RPTADV_ECHOLINK_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

#define RPTADV_ECHOLINK_ABI_VERSION 1U /**< Descriptor and sized-record ABI. */
/** Fixed-width operation result; Full consumes no input. */
typedef uint32_t rptadv_echolink_status;
/** Defined result values; Pending does not indicate failure. */
enum rptadv_echolink_result {
    RPTADV_ECHOLINK_OK = 0, /**< Operation complete. */
    RPTADV_ECHOLINK_PENDING = 1, /**< Waiting for readiness. */
    RPTADV_ECHOLINK_FULL = 2, /**< No capacity; no input consumed. */
    RPTADV_ECHOLINK_UNSUPPORTED = 3, /**< Capability unavailable. */
    RPTADV_ECHOLINK_INVALID = 4, /**< Invalid argument or sized record. */
    RPTADV_ECHOLINK_CLOSED = 5, /**< Session is closed. */
    RPTADV_ECHOLINK_FAILED = 6 /**< Unrecoverable owner/setup failure. */
};
/** Opaque provider runtime owner. */
struct rptadv_echolink_context;
/** Opaque account; it retains runtime ownership. */
struct rptadv_echolink_account;
/** Opaque provider-created QSO. */
struct rptadv_echolink_qso;
/** Opaque serial-step registration. */
struct rptadv_echolink_registration;

/** Borrowed bytes; a nonempty span requires a readable nonnull pointer. */
struct rptadv_echolink_bytes_v1 {
    const uint8_t *data; /**< Readable bytes. */
    size_t length; /**< Number of readable bytes. */
};
/** Borrowed contiguous list of byte spans. */
struct rptadv_echolink_strings_v1 {
    const struct rptadv_echolink_bytes_v1 *items; /**< Null only when count is zero. */
    size_t count; /**< Number of entries. */
};
/** Shared pool setup; never a runtime per account or QSO. */
struct rptadv_echolink_runtime_config_v1 {
    uint32_t struct_size; /**< Complete readable record size. */
    uint32_t abi_version; /**< One. */
    uint32_t worker_threads; /**< 1..256; zero selects available CPUs clamped to 1..4. */
    /** Optional complete resource tail. All empty keeps lifecycle-only preparation.
     * GSM and jitter are required together for enabled accounts. The original
     * 12-byte lifecycle prefix remains accepted without this tail. */
    struct rptadv_echolink_bytes_v1 gsm_library;
    struct rptadv_echolink_bytes_v1 speex_library; /**< Optional narrowband Speex DSO. */
    struct rptadv_echolink_bytes_v1 jitter_library; /**< Released packet-jitter adapter DSO. */
};
/** Advertised metadata, not hardware-control commands. */
struct rptadv_echolink_station_v1 {
    struct rptadv_echolink_bytes_v1 name; /**< UTF-8 station name. */
    struct rptadv_echolink_bytes_v1 location; /**< UTF-8 location. */
    struct rptadv_echolink_bytes_v1 email; /**< Contact email. */
    struct rptadv_echolink_bytes_v1 status_message; /**< Station information. */
    double latitude; /**< Decimal degrees north. */
    double longitude; /**< Decimal degrees east. */
    double frequency_mhz; /**< Advertised RF frequency. */
    double tone_hz; /**< Advertised CTCSS tone. */
    double power_w; /**< Advertised RF power in watts. */
    double antenna_height_m; /**< Antenna height in metres. */
    double antenna_gain_db; /**< Advertised antenna gain. */
    double antenna_direction_deg; /**< Antenna direction in degrees. */
};
/** Selected transport; passwords are separate. */
struct rptadv_echolink_transport_v1 {
    uint32_t kind; /**< Direct=0, standard TCP Proxy=1. */
    struct rptadv_echolink_bytes_v1 bind_address; /**< Local IPv4 address. */
    uint16_t audio_port; /**< Direct audio port; control is audio_port+1. */
    uint16_t proxy_port; /**< Proxy TCP port, normally 8100. */
    struct rptadv_echolink_bytes_v1 proxy_host; /**< Proxy hostname. */
    struct rptadv_echolink_strings_v1 directory_servers; /**< Ordered directory hosts. */
};
/** Resolved account limits, deadlines, gains and announcement policy. */
struct rptadv_echolink_policy_v1 {
    uint32_t max_stations; /**< Admitted-QSO limit. */
    uint32_t max_pending_handshakes; /**< Separate unauthenticated budget. */
    uint32_t heartbeat_interval_ms; /**< Control-heartbeat interval. */
    uint32_t heartbeat_timeout_ms; /**< Valid-packet liveness timeout. */
    uint32_t connect_timeout_ms; /**< QSO setup timeout. */
    uint32_t directory_timeout_ms; /**< Directory operation deadline. */
    uint32_t registration_refresh_ms; /**< Login refresh interval. */
    uint32_t directory_refresh_ms; /**< Snapshot refresh interval. */
    uint32_t directory_retry_ms; /**< Failed-directory retry interval. */
    uint32_t directory_freshness_ms; /**< Maximum admission age. */
    double receive_gain_db; /**< Apply once at ingress. */
    double transmit_gain_db; /**< Apply once at egress. */
    uint32_t announcement; /**< Off=0, node=1, callsign=2, both=3. */
    uint32_t telemetry; /**< Off=0, on=1, follow=2, demand=3. */
    uint32_t telemetry_dynamic; /**< Nonzero allows authorized runtime selection. */
    struct rptadv_echolink_strings_v1 allow; /**< Callsign wildcard allow rules. */
    struct rptadv_echolink_strings_v1 deny; /**< Deny overrides allow. */
};
/** Fully resolved per-node configuration, borrowed for the operation. */
struct rptadv_echolink_account_config_v1 {
    uint32_t struct_size; /**< Complete readable record size. */
    uint32_t abi_version; /**< One. */
    uint32_t enabled; /**< Disabled=0, enabled=1. */
    uint32_t node_number; /**< Complete real ID, not an alias. */
    struct rptadv_echolink_bytes_v1 local_node; /**< Owning controller node. */
    struct rptadv_echolink_bytes_v1 callsign; /**< Validated callsign. */
    struct rptadv_echolink_station_v1 station; /**< Advertised metadata. */
    struct rptadv_echolink_transport_v1 transport; /**< Selected transport. */
    struct rptadv_echolink_policy_v1 policy; /**< Resolved policy. */
};
/** Account-specific secrets, never printable. */
struct rptadv_echolink_secrets_v1 {
    uint32_t struct_size; /**< Complete readable record size. */
    uint32_t abi_version; /**< One. */
    struct rptadv_echolink_bytes_v1 directory_password; /**< Directory password. */
    struct rptadv_echolink_bytes_v1 proxy_password; /**< Distinct Proxy password. */
};
/** Immutable connection identity; copy spans before destroying the QSO. */
struct rptadv_echolink_peer_info_v1 {
    uint32_t struct_size; /**< Caller-provided writable size. */
    uint32_t abi_version; /**< One. */
    uint32_t node_number; /**< Complete real EchoLink ID. */
    uint32_t capabilities; /**< Per-connection capabilities. */
    uint64_t account_generation; /**< Owning account generation. */
    uint64_t qso_generation; /**< Exact connection generation. */
    struct rptadv_echolink_bytes_v1 callsign; /**< Provider-validated callsign. */
    struct rptadv_echolink_bytes_v1 local_node; /**< Owning controller node. */
};
/** Borrowed callback event; never a caller-supplied authentication proof. */
struct rptadv_echolink_event_v1 {
    uint32_t struct_size; /**< Complete record size. */
    uint32_t kind; /**< Status=0, pending QSO=1, terminal QSO=2. */
    struct rptadv_echolink_qso *qso; /**< Provider-owned pending/live QSO or null. */
    uint64_t generation; /**< Account/QSO generation appropriate to kind. */
    rptadv_echolink_status status; /**< Value-free operation/termination status. */
};
/** Off-audio event notification; must not unwind or retain borrowed records. */
typedef void (*rptadv_echolink_event_fn)(void *, const struct rptadv_echolink_event_v1 *);
/** Serial peer turn; negative stops notification; no peer borrow spans the call. */
typedef int32_t (*rptadv_echolink_step_fn)(void *);

/** Immutable table; unavailable slots are not advertised as capabilities. */
struct rptadv_echolink_descriptor_v1 {
    uint32_t struct_size; /**< Complete readable table size. */
    uint32_t abi_version; /**< One. */
    uint8_t capability[24]; /**< NUL-padded rptadv.echolink.v1. */
    uint64_t capabilities; /**< Lifecycle=1, network=2, prepared=4, text=8. */
    /** Create off audio; failure clears output. */
    rptadv_echolink_status (*context_create)(const struct rptadv_echolink_runtime_config_v1 *, struct rptadv_echolink_context **);
    /** Release caller ownership; accounts retain their own runtime references. */
    void (*context_destroy)(struct rptadv_echolink_context *);
    /** Copy/validate configuration without login; failure clears output. */
    rptadv_echolink_status (*account_prepare)(struct rptadv_echolink_context *, const struct rptadv_echolink_account_config_v1 *, const struct rptadv_echolink_secrets_v1 *, struct rptadv_echolink_account **);
    /** Start I/O; incoming event ownership is consumed/retained explicitly. */
    rptadv_echolink_status (*account_activate)(struct rptadv_echolink_account *, rptadv_echolink_event_fn, void *);
    /** Prepare/commit an account-only generation change. */
    rptadv_echolink_status (*account_reconfigure)(struct rptadv_echolink_account *, const struct rptadv_echolink_account_config_v1 *, const struct rptadv_echolink_secrets_v1 *);
    /** Gate admissions/stop; repeated calls are harmless. */
    rptadv_echolink_status (*account_stop)(struct rptadv_echolink_account *);
    /** Release after callback quiescence; null is harmless. */
    void (*account_destroy)(struct rptadv_echolink_account *);
    /** Dial a real ID; failure clears output. */
    rptadv_echolink_status (*dial)(struct rptadv_echolink_account *, uint32_t, struct rptadv_echolink_qso **);
    /** Recheck bound pending admission; never trust a caller-set verified flag. */
    rptadv_echolink_status (*validate_pending)(const struct rptadv_echolink_qso *, struct rptadv_echolink_peer_info_v1 *);
    /** Copy immutable identity/capabilities. */
    rptadv_echolink_status (*peer_info)(const struct rptadv_echolink_qso *, struct rptadv_echolink_peer_info_v1 *);
    /** Borrow serial peer ownership; callback may run before return. */
    rptadv_echolink_status (*peer_attach)(struct rptadv_echolink_context *, struct rptadv_echolink_qso *, rptadv_echolink_step_fn, void *, struct rptadv_echolink_registration **);
    /** Nonblocking callback-safe stop. */
    void (*peer_stop)(struct rptadv_echolink_registration *);
    /** Wait off executor for quiescence before releasing the context/peer. */
    void (*peer_detach)(struct rptadv_echolink_registration *);
    /** Wake the existing owner without a task per packet. */
    void (*peer_wake)(struct rptadv_echolink_registration *);
    /** Relative nanosecond product deadline; UINT64_MAX clears it. */
    rptadv_echolink_status (*peer_set_deadline)(struct rptadv_echolink_qso *, uint64_t);
};
/** Borrow the immutable table while the DSO remains loaded. */
const struct rptadv_echolink_descriptor_v1 *rptadv_echolink_descriptor_v1(void);
#ifdef __cplusplus
}
#endif
#endif
