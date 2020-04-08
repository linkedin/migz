/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import org.junit.Test;

import static org.junit.Assert.*;


public class MiGzTest {
  /**
   * Tests the compression of (almost always) incompressible pseudorandom data.  This is useful for exercising the edge
   * case wherein DEFLATE stores data in uncompressed blocks that are larger than the original data.
   *
   * @throws IOException nominally, but not in practice since in-memory streams are used
   */
  @Test
  public void testRandomDataCompression() throws IOException {
    Random r = new Random(1);
    byte[][] buffers = new byte[][]{new byte[100], new byte[1000], new byte[10000], new byte[100000]};

    for (int i = 0; i < 1000; i++) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          MiGzOutputStream mzos = new MiGzOutputStream(baos, MiGzOutputStream.DEFAULT_THREAD_COUNT, 50 * 1024)) {
        mzos.setCompressionLevel(3);

        for (int j = 0; j < 10; j++) {
          for (byte[] buffer : buffers) {
            r.nextBytes(buffer);
            mzos.write(buffer);
          }
        }
      }
    }
  }

  @Test
  public void testOutput() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    MiGzOutputStream mzos = new MiGzOutputStream(baos, 16, 2);
    mzos.write(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
    mzos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    MiGzInputStream mzip = new MiGzInputStream(bais, 20);

    for (int i = 0; i < 16; i++) {
      System.out.println(mzip.read());
    }
  }

  @Test
  public void testShakeswordWithVaryingFlushRates() throws IOException {
    testShakesword(0, 100);
    testShakesword(0, 1000);
    testShakesword(0.02, 1000);
    testShakesword(0.05, 1000);
    testShakesword(0.2, 1000);
    testShakesword(0.8, 1000);
  }

  public void testShakesword(double flushRate, int blockSize) throws IOException {
    java.io.InputStream shakestream = MiGzTest.class.getResourceAsStream("/shakespeare.tar");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    MiGzOutputStream mzos = new MiGzOutputStream(baos, 8, blockSize);
    byte[] buffer = new byte[1024 * 16];
    int read;
    Random r = new Random(1337);

    while ((read = shakestream.read(buffer)) > 0) {
      mzos.write(buffer, 0, read);
      if (r.nextDouble() < flushRate) {
        mzos.flush();
      }
    }
    mzos.close();

    ByteArrayInputStream bais1 = new ByteArrayInputStream(baos.toByteArray());
    GZIPInputStream gzis = new GZIPInputStream(bais1);

    ByteArrayInputStream bais2 = new ByteArrayInputStream(baos.toByteArray());
    MiGzInputStream mzis = new MiGzInputStream(bais2, 10);

    shakestream = MiGzTest.class.getResourceAsStream("/shakespeare.tar");

    int byte1;
    int byte2;
    int byte3;

    int count = 0;
    do {
      byte1 = shakestream.read();
      byte2 = mzis.read();
      byte3 = gzis.read();

      assertEquals("Error reading with MiGzInputStream on byte " + (count++), byte1, byte2);
      assertEquals("Error reading with GZipInputStream on byte " + (count++), byte1, byte3);
    } while (byte1 != -1);
  }

  private static void copyShakestream(OutputStream target) throws IOException {
    try (InputStream shakestream = MiGzTest.class.getResourceAsStream("/shakespeare.tar")) {
      byte[] buffer = new byte[1024 * 16];
      int read;

      while ((read = shakestream.read(buffer)) > 0) {
        target.write(buffer, 0, read);
      }
    }
  }

  @Test
  public void decompressionSpeedTest() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (MiGzOutputStream mzos = new MiGzOutputStream(baos).setCompressionLevel(Deflater.DEFAULT_COMPRESSION)) {
      copyShakestream(mzos);
    }

    long time = System.currentTimeMillis();
    byte[] readBuffer = new byte[512 * 1024];
    for (int i = 0; i < 100; i++) {
      ByteArrayInputStream bais2 = new ByteArrayInputStream(baos.toByteArray());
      //GZIPInputStream mzis = new GZIPInputStream(bais2);
      MiGzInputStream mzis = new MiGzInputStream(bais2);
      while (mzis.read(readBuffer) > 0) {

      }
    }
    System.out.println(System.currentTimeMillis() - time);
  }

  @Test
  public void compressionExceptionTest() throws IOException {
    OutputStream os = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        throw new ClassCastException();
      }
    };

    MiGzOutputStream mos = new MiGzOutputStream(os);
    try {
      while (true) {
        mos.write(4);
      }
    } catch (ClassCastException e) {
      return;
    }
  }

  @Test
  public void decompressionExceptionTest() throws IOException {
    InputStream is = new InputStream() {
      @Override
      public int read() throws IOException {
        throw new ClassCastException();
      }
    };

    MiGzInputStream mis = new MiGzInputStream(is);
    try {
      while (true) {
        mis.readBuffer();
      }
    } catch (ClassCastException e) {
      mis.close();
      return;
    }
  }
}
