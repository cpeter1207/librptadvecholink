/** @file
 * @brief Fault boundary around a retained released codec, never a substitute
 * codec. Follows rptadv-codec-adapters' codec_loader_fixture.c descriptor-copy
 * pattern.
 */
#include <rptadv-codec/rptadv_codec.h>

static rptadv_codec_descriptor_v1_table original, fixture;
static int fault;
static size_t decodes, conceals;

/** Fail a new context without replacing or leaking an existing codec owner. */
static int32_t open_encoder(const rptadv_codec_config_v1 *config,
                            void **context) {
  if (fault == 7)
    return RPTADV_CODEC_FAILED;
  return original.open_encoder(config, context);
}
static int32_t open_decoder(const rptadv_codec_config_v1 *config,
                            void **context) {
  if (fault == 8)
    return RPTADV_CODEC_FAILED;
  return original.open_decoder(config, context);
}
/** A coding-history reset is fallible even on an established connection. */
static int32_t reset(void *context) {
  if (fault == 9)
    return RPTADV_CODEC_FAILED;
  return original.reset(context);
}

/** Preserve inspection except for explicit provider-failure/corruption tests.
 */
static int32_t inspect(void *context, const uint8_t *packet, size_t length,
                       rptadv_codec_packet_info_v1 *result) {
  if (fault == 1)
    return RPTADV_CODEC_FAILED;
  if (fault == 6)
    return RPTADV_CODEC_MALFORMED;
  return original.inspect(context, packet, length, result);
}

/** Inject damage or bounded result faults while retaining real codec history.
 */
static int32_t decode(void *context, const uint8_t *packet, size_t length,
                      float *pcm, size_t capacity, size_t *frames) {
  ++decodes;
  if (fault == 2)
    return RPTADV_CODEC_MALFORMED;
  if (fault == 5)
    return RPTADV_CODEC_FAILED;
  int32_t result =
      original.decode(context, packet, length, pcm, capacity, frames);
  if (result == RPTADV_CODEC_COMPLETE) {
    if (fault == 3)
      *frames = 0;
    if (fault == 4)
      *frames = 639;
  }
  return result;
}

/** Native PLC remains the released provider's implementation, including
 * Unsupported. */
static int32_t conceal(void *context, uint32_t count, float *pcm,
                       size_t capacity, size_t *frames) {
  ++conceals;
  return original.conceal(context, count, pcm, capacity, frames);
}

/** Exercise packetizer checks after the neutral client accepts bounded
 * metadata. */
static int32_t encode(void *context, const float *pcm, size_t frames,
                      uint8_t *packet, size_t capacity, uint32_t flush,
                      rptadv_codec_encode_result_v1 *result) {
  int32_t status =
      original.encode(context, pcm, frames, packet, capacity, flush, result);
  if (status == RPTADV_CODEC_COMPLETE) {
    if (fault == 10)
      result->consumed_frames = 639;
    if (fault == 11) {
      result->packet_frames = 320;
      result->source_frames = 320;
    }
    if (fault == 12)
      result->encoded_bytes = 0;
  }
  return status;
}

/** Caller retains the real library through every derived context. */
void rptadv_test_configure(const void *descriptor) {
  original = *(const rptadv_codec_descriptor_v1_table *)descriptor;
  fixture = original;
  fixture.open_encoder = open_encoder;
  fixture.open_decoder = open_decoder;
  fixture.reset = reset;
  fixture.inspect = inspect;
  fixture.decode = decode;
  fixture.conceal = conceal;
  fixture.encode = encode;
}

/** Select a fault only after normal catalog validation/open has succeeded. */
void rptadv_test_fault(int selected) {
  fault = selected;
  decodes = conceals = 0;
}

/** Count actual codec operations, not retries at the product sink. */
size_t rptadv_test_calls(uint32_t plc) { return plc ? conceals : decodes; }

/** Normal released descriptor entry point consumed by CodecCatalog. */
const rptadv_codec_descriptor_v1_table *rptadv_codec_descriptor_v1(void) {
  return &fixture;
}
