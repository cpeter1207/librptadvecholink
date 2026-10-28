/* SPDX-License-Identifier: GPL-2.0-only */
/** @file @brief Installed descriptor lifecycle and layout smoke check. */
#include "rptadv_echolink.h"
#include <assert.h>
#include <string.h>

int main(void)
{
    const struct rptadv_echolink_descriptor_v1 *api = rptadv_echolink_descriptor_v1();
    assert(api != NULL);
    assert(api->struct_size == sizeof(*api));
    assert(api->abi_version == 1);
    assert(memcmp(api->capability, "rptadv.echolink.v1", 19) == 0);
    struct rptadv_echolink_runtime_config_v1 config = {0};
    config.struct_size = sizeof(config);
    config.abi_version = 1;
    struct rptadv_echolink_context *context = NULL;
    assert(api->context_create(&config, &context) == RPTADV_ECHOLINK_OK);
    struct rptadv_echolink_account_config_v1 account_config = {0};
    account_config.struct_size = sizeof(account_config);
    account_config.abi_version = 1;
    struct rptadv_echolink_account *account = NULL;
    assert(api->account_prepare(context, &account_config, NULL, &account) == RPTADV_ECHOLINK_OK);
    assert(account != NULL);
    api->context_destroy(context);
    assert(api->account_stop(account) == RPTADV_ECHOLINK_OK);
    api->account_destroy(account);
    api->account_destroy(NULL);
    return 0;
}
