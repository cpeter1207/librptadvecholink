/* SPDX-License-Identifier: GPL-2.0-only */
/** @file Independent four-frame upstream libspeex wire probe; test-only. */
#include <speex/speex.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
  if (argc != 2)
    return 2;
  const int encode = strcmp(argv[1], "encode") == 0;
  void *state = encode ? speex_encoder_init(&speex_nb_mode)
                       : speex_decoder_init(&speex_nb_mode);
  if (!state)
    return 2;
  SpeexBits bits;
  speex_bits_init(&bits);
  int16_t pcm[640];
  char payload[252];
  int result = 0;
  if (encode) {
    int bitrate = 25000;
    int quality = 8;
    int complexity = 4;
    if (speex_encoder_ctl(state, SPEEX_SET_BITRATE, &bitrate) ||
        speex_encoder_ctl(state, SPEEX_SET_QUALITY, &quality) ||
        speex_encoder_ctl(state, SPEEX_SET_COMPLEXITY, &complexity) ||
        fread(pcm, sizeof(*pcm), 640, stdin) != 640) {
      result = 2;
    } else {
      for (unsigned i = 0; i < 640; i += 160)
        if (speex_encode_int(state, pcm + i, &bits) < 0)
          result = 2;
      speex_bits_insert_terminator(&bits);
      const int count = speex_bits_write(&bits, payload, sizeof(payload));
      if (count <= 0 ||
          fwrite(payload, 1, (size_t)count, stdout) != (size_t)count)
        result = 2;
    }
  } else {
    const size_t count = fread(payload, 1, sizeof(payload), stdin);
    speex_bits_read_from(&bits, payload, (int)count);
    for (unsigned i = 0; i < 640; i += 160)
      if (speex_decode_int(state, &bits, pcm + i) != 0) {
        result = 2;
        break;
      }
    int16_t extra[160];
    if (!result && speex_decode_int(state, &bits, extra) != -1)
      result = 2;
    if (!result && fwrite(pcm, sizeof(*pcm), 640, stdout) != 640)
      result = 2;
  }
  speex_bits_destroy(&bits);
  if (encode)
    speex_encoder_destroy(state);
  else
    speex_decoder_destroy(state);
  return result;
}
