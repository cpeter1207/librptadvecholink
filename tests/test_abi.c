/* SPDX-License-Identifier: GPL-2.0-only */
/** @file @brief Installed descriptor lifecycle and layout smoke check. */
#include "rptadv_echolink.h"
#include <assert.h>
#include <string.h>

_Static_assert(offsetof(struct rptadv_echolink_event_v1, taken) == 32,
               "incoming ownership acknowledgement layout");
_Static_assert(sizeof(struct rptadv_echolink_event_v1) == 40,
               "complete borrowed incoming event layout");
_Static_assert(offsetof(struct rptadv_echolink_peer_info_v1, source) == 64,
               "observed incoming endpoint tail layout");
_Static_assert(sizeof(struct rptadv_echolink_peer_info_v1) == 80,
               "complete verified identity record layout");
_Static_assert(sizeof(struct rptadv_echolink_read_result_v1) == 24,
               "source-rate read result layout");
_Static_assert(offsetof(struct rptadv_echolink_descriptor_v1, account_apply_prepared) ==
                   offsetof(struct rptadv_echolink_descriptor_v1, peer_send_text) +
                       sizeof(((struct rptadv_echolink_descriptor_v1 *)0)->peer_send_text),
               "prepared apply preserves the preceding text group");
_Static_assert(offsetof(struct rptadv_echolink_read_result_v1, finish_pending) == 16,
               "finite-completion request layout");
_Static_assert(offsetof(struct rptadv_echolink_descriptor_v1, peer_read) ==
                   offsetof(struct rptadv_echolink_descriptor_v1, peer_destroy) +
                       sizeof(((struct rptadv_echolink_descriptor_v1 *)0)->peer_destroy),
               "media tail preserves the network prefix");

int main(int argc, char **argv)
{
    const struct rptadv_echolink_descriptor_v1 *api = rptadv_echolink_descriptor_v1();
    assert(api != NULL);
    assert(api->struct_size == sizeof(*api));
    assert(api->abi_version == 1);
    assert(memcmp(api->capability, "rptadv.echolink.v1", 19) == 0);
    struct rptadv_echolink_runtime_config_v1 config = {0};
    config.struct_size = sizeof(config);
    config.abi_version = 1;
    assert(argc == 1 || argc == 4);
    if (argc == 4) {
        config.worker_threads = 1;
        config.gsm_library = (struct rptadv_echolink_bytes_v1){
            (const uint8_t *)argv[1], strlen(argv[1])};
        config.speex_library = (struct rptadv_echolink_bytes_v1){
            (const uint8_t *)argv[2], strlen(argv[2])};
        config.jitter_library = (struct rptadv_echolink_bytes_v1){
            (const uint8_t *)argv[3], strlen(argv[3])};
    }
    struct rptadv_echolink_context *context = NULL;
    assert(api->context_create(&config, &context) == RPTADV_ECHOLINK_OK);
    struct rptadv_echolink_account_config_v1 account_config = {0};
    account_config.struct_size = sizeof(account_config);
    account_config.abi_version = 1;
    struct rptadv_echolink_account *account = NULL;
    assert(api->account_prepare(context, &account_config, NULL, &account) == RPTADV_ECHOLINK_OK);
    assert(account != NULL);
    struct rptadv_echolink_account *prepared = NULL;
    assert(api->account_prepare(context, &account_config, NULL, &prepared) == RPTADV_ECHOLINK_OK);
    assert(api->account_apply_prepared != NULL);
    assert(api->account_apply_prepared(account, prepared) == RPTADV_ECHOLINK_OK);
    assert(api->account_apply_prepared(account, prepared) == RPTADV_ECHOLINK_OK);
    api->context_destroy(context);
    assert(api->account_stop(account) == RPTADV_ECHOLINK_OK);
    api->account_destroy(account);
    api->account_destroy(prepared);
    api->account_destroy(NULL);
    return 0;
}
