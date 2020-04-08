/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReference;


class MiGzUtil {
  private MiGzUtil() { }

  // From the GZip RFC:
  // +---+---+---+---+---+---+---+---+---+---+
  // |ID1|ID2|CM |FLG|     MTIME     |XFL|OS |
  // +---+---+---+---+---+---+---+---+---+---+
  //
  // +---+---+=================================+
  // | XLEN  |...XLEN bytes of "extra field"...|
  // +---+---+=================================+
  //
  // +---+---+---+---+==================================+
  // |SI1|SI2|  LEN  |... LEN bytes of subfield data ...|
  // +---+---+---+---+==================================+
  public static final byte[] GZIP_HEADER = {
      0x1f, // first magic byte
      (byte) 0x8b, // second magic byte
      8, // deflate
      4, // flags: EXTRA field is present
      0, // MTIME = 0 (no timestamp available)
      0,
      0,
      0,
      2, // Max compression
      0, // OS = whatever

      8, // _length of extra fields LSB
      0, // MSB

      'M', // MiGz magic bytes
      'Z',
      4, // extra MZ field size LSB
      0, // MSB

      // After this comes:
      // Compressed size (4 bytes)
      // compressed data ([compressed size] bytes)
      // CRC (4 bytes)
      // uncompressed size (4 bytes)
  };

  public static final int GZIP_HEADER_SIZE = GZIP_HEADER.length + 4;
  public static final int GZIP_FOOTER_SIZE = 8;

  public static final int DEFAULT_BLOCK_SIZE = 512 * 1024;

  public static int maxCompressedSize(int uncompressed) {
    // zlib (which backs all JVM implementations of Java's Deflate class that we are aware of) will use a memLevel of 8
    // for the DEFLATE algorithm by default, which translates to an uncompressable block size of 2^14 - 1 (~16KB).
    // However, the minimum memLevel permitted by zlib is 1, which translates to a block size of 2^7 - 1 (127 bytes).
    // Because we have no portable way to detect the memLevel that will be used in the current JVM with absolute
    // certainty, we assume the worst-case block size of 127 (even though in practice it is a *virtual* certainty it
    // will be 16,383).  This results in slightly higher than ideal memory consumption (by about 4%).
    //
    // We note that, in principle, nothing stops another, non-zlib DEFLATE implementation from allowing an even smaller
    // block size; however, no plausible/sane Deflate class would ever use such a small size.
    final int deflateBlockSize = 127; // absolute minimum, worst-case DEFLATE block size allowed by zlib

    // According to the relevant RFC, Deflate should add no more than 5 bytes per DEFLATE block;
    // we add one additional byte to avoid a potential issue where Deflate does not properly report a "finished" block
    // if the size of the deflated block is exactly the size of the available buffer.
    return uncompressed + ((uncompressed + deflateBlockSize - 1) / deflateBlockSize) * 5 + 1;
  }

  public static void checkException(AtomicReference<RuntimeException> exception) throws IOException {
    if (exception.get() != null) {
      RuntimeException re = exception.get();
      if (re instanceof UncheckedIOException) {
        throw ((UncheckedIOException) re).getCause();
      }
      throw re;
    }
  }
}
