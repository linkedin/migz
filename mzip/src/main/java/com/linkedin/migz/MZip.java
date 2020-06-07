/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import java.io.IOException;


/**
 * Compresses stdin using the MiGz format and writes the result to stdout.
 * Stderr is used to output performance information.
 */
public class MZip  {
  private MZip() { }

  public static void main(String[] args) throws IOException {
    int threadCount = Runtime.getRuntime().availableProcessors();
    int blockSize = MiGzUtil.DEFAULT_BLOCK_SIZE;
    int compressionLevel = 9;

    // CHECKSTYLE:OFF
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-t")) {
        threadCount = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-b")) {
        blockSize = Integer.parseInt(args[++i]);
      } else if (args[i].startsWith("-") && args[i].length() == 2 && Character.isDigit(args[i].charAt(1))) {
        compressionLevel = Integer.parseInt(args[i].substring(1));
      } else if (args[i].contains("help") || args[i].contains("?")) {
        System.out.println("Compresses data from stdin and outputs the GZip-compressed bytes to stdout.");
        System.out.println("Compressed data may be decompressed with any GZip utility single-threaded, or use MiGz "
            + "to decompress it using multiple threads");
        System.out.println("Optional arguments:");
        System.out.println(
            "\t-t [thread count] : sets the number of threads to use (default = 2 * number of logical cores)");
        System.out.println(
            "\t-b : sets the block size, in bytes (default = 512KB)");
        System.out.println("\t-0, -1, -2...-#...-9 : sets the compression level (0 = no compression, 1 = fastest "
            + "compression, 9 = best compression; default = 9)");
      } else {
        throw new IllegalArgumentException("Unrecognized command line argument: " + args[i]);
      }
    }
    // CHECKSTYLE:ON

    System.err.println(
        "Compressing stdin using " + threadCount + " threads, blocks of size " + blockSize + ", and compression level "
            + compressionLevel);

    long startTime = System.nanoTime();

    byte[] buffer = new byte[blockSize];
    int readCount;
    MiGzOutputStream mzos =
        new MiGzOutputStream(System.out, threadCount, blockSize).setCompressionLevel(compressionLevel);

    while ((readCount = System.in.read(buffer)) > 0) {
      mzos.write(buffer, 0, readCount);
    }

    mzos.close();

    double timeInSeconds = ((double) (System.nanoTime() - startTime)) / (1000 * 1000 * 1000);
    System.err.println("Compression completed in " + timeInSeconds + " seconds");
  }
}
