/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;


/**
 * Decompresses stdin using the MiGz format and writes the result to stdout.
 * Stderr is used to output performance information.
 */
public class MUnzip {
  private MUnzip() { }

  public static void main(String[] args) throws IOException {
    int threadCount = Runtime.getRuntime().availableProcessors();

    // used for perf testing
    boolean gzip = false;

    // CHECKSTYLE:OFF
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-t")) {
        threadCount = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-gzip")) {
        // standard gzip, for perf testing
        gzip = true;
      } else if (args[i].contains("help") || args[i].contains("?")) {
        System.out.println("Decompresses MiGz-compressed data from stdin and outputs the decompressed bytes to stdout");
        System.out.println("Optional arguments:");
        System.out.println(
            "\t-t [thread count] : sets the number of threads to use (default = 2 * number of logical cores)");
        System.out.println(
            "\t-gzip : use standard, non-multithreaded gzip to decompress (used for performance comparisons)");
      } else {
        throw new IllegalArgumentException("Unrecognized command line argument: " + args[i]);
      }
    }
    // CHECKSTYLE:ON

    System.err.println("Decompressing stdin using " + threadCount + " threads");

    long startTime = System.nanoTime();

    if (gzip) {
      InputStream gzipIS = new GZIPInputStream(System.in, MiGzUtil.DEFAULT_BLOCK_SIZE);
      byte[] buffer = new byte[MiGzUtil.DEFAULT_BLOCK_SIZE];
      int readCount;

      while ((readCount = gzipIS.read(buffer)) > 0) {
        System.out.write(buffer, 0, readCount);
      }
    } else {
      MiGzInputStream mzis = new MiGzInputStream(System.in, threadCount);
      MiGzBuffer buffer;

      while ((buffer = mzis.readBuffer()) != null) {
        System.out.write(buffer.getData(), 0, buffer.getLength());
      }
    }

    System.out.close();

    double timeInSeconds = ((double) (System.nanoTime() - startTime)) / (1000 * 1000 * 1000);
    System.err.println("Decompression completed in " + timeInSeconds + " seconds");
  }
}
