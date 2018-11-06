/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import com.concurrentli.EpochEvent;
import com.concurrentli.FutureEpochEvent;
import com.concurrentli.UncheckedInterruptedException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;


/**
 * MiGzOutputStream writes data to an underlying stream in gzip format using multiple threads.
 * This stream can then be read by any compliant gzip decompression utility or library (including GZipInputStream), but
 * should be read by MiGzInputStream for fastest decompression.
 *
 * MiGz breaks the data into blocks, compresses each block in parallel, and then writes them as them multiple gzip
 * "records" in serial as per the GZip specification (this is perhaps not a widely known feature, but it's why you can
 * concatenate two gzip'ed files and the result will decompress to the concatenation of the original data).
 *
 * MiGz-compressed streams can be concatenated together and read back by MiGzInputStream to recover the original
 * concatenated data.
 *
 * With the default block size, there is experimentally a very small (~1%) penalty for using MiGz vs. traditional
 * GZip, mostly as a consequence of compressing data in chunks rather than holistically, and slightly due to the cost
 * of writing extra GZip record headers (these are very small).  The default block size is chosen to keep the total
 * compressed size extremely close to normal GZip at maximum compression and still allow a high degree of
 * parallelization.
 */
public class MiGzOutputStream extends OutputStream {
  /**
   * The class operates as follows:
   * (1) Writes go into a buffer; these buffers are reused and held in a buffer pool.
   *     Writes block if there are presently no available buffers in the buffer pool.
   * (2) When a block's worth of data is available, compression is scheduled in the thread pool.
   * (3) A thread from a thread pool compresses the block.
   * (4) The thread waits for its turn to write to the underlying stream, then writes the header and compressed data
   */
  public static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
  public static final int DEFAULT_BLOCK_SIZE = MiGzUtil.DEFAULT_BLOCK_SIZE;
  public static final int DEFAULT_COMPRESSION_LEVEL = Deflater.BEST_COMPRESSION;

  private long _currentBlock = 0; // the index of the block currently being written by calls to write(...)
  private AtomicReference<RuntimeException> _exception = new AtomicReference<>(null);

  // the current buffer being filled by calls to write(...), and the offset at which additional bytes should be written
  private byte[] _currentBuffer;
  private int _currentBufferOffset = 0;

  private int _compressionLevel = DEFAULT_COMPRESSION_LEVEL; // [-1, 9] value specifying the Deflater compression level

  private final ArrayBlockingQueue<byte[]> _writeBufferPool; // reuse byte arrays to avoid creation/GC overhead

  private final int _blockSize; // the number of bytes in the original, uncompressed blocks
  private final int _threads; // number of threads used to write the original MiGzipped file

  // controls writes to the wrapped output stream:
  private final EpochEvent _outputEpoch = new FutureEpochEvent(0);
  private final OutputStream _outputStream; // final destination of compressed bytes
  private final Object _outputStreamLock = new Object(); // unnecessary lock being used for debugging

  private final ExecutorService _threadPool; // compressor threads

  // per-thread buffer for holding compressed data
  private final ThreadLocal<byte[]> _compressedBuffer = new ThreadLocal<byte[]>() {
    @Override protected byte[] initialValue() {
      byte[] res =
          new byte[MiGzUtil.GZIP_HEADER_SIZE + _maxCompressedSize + MiGzUtil.GZIP_FOOTER_SIZE];
      System.arraycopy(MiGzUtil.GZIP_HEADER, 0, res, 0, MiGzUtil.GZIP_HEADER.length);
      return res;
    }
  };

  private final int _maxCompressedSize; // what's the maximum, worst-case number of bytes a compressed block can occupy?
  private final byte[] _minibuffer = new byte[1]; // used for single-byte writes

  /**
   * Creates a new MiGzOutputStream that will output MiGz-compressed bytes to the specified underlying
   * outputStream using the default blocksize and thread count.
   *
   * @param outputStream the stream to which compressed bytes will be written
   */
  public MiGzOutputStream(OutputStream outputStream) {
    this(outputStream, DEFAULT_THREAD_COUNT, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Creates a new MiGzOutputStream that will output MiGz-compressed bytes to the specified underlying
   * outputStream.
   *
   * @param outputStream the stream to which compressed bytes will be written
   * @param threads the number of threads to use for compression
   * @param blockSize the number of bytes that comprise each block to be compressed; larger blocks result in better
   *                  compression at the expense of more RAM usage when compressing and decompressing.
   */
  public MiGzOutputStream(OutputStream outputStream, int threads, int blockSize) {
    _outputStream = outputStream;
    _maxCompressedSize = MiGzUtil.maxCompressedSize(blockSize);
    _threads = threads;
    _blockSize = blockSize;
    _writeBufferPool = new ArrayBlockingQueue<>(_threads, false,
        IntStream.range(1, _threads).mapToObj(i -> new byte[_blockSize]).collect(Collectors.toList()));
    _threadPool = Executors.newFixedThreadPool(_threads);
    _currentBuffer = new byte[_blockSize];
  }

  /**
   * Sets the GZip compression level used.  This value may be -1 for GZip's default compression level
   * ({@link Deflater#DEFAULT_COMPRESSION}), 0 for "no compression" (({@link Deflater#NO_COMPRESSION}), 1 for
   * fastest compression (({@link Deflater#BEST_SPEED}), 9 for best compression ({@link Deflater#BEST_COMPRESSION}),
   * or 2-8 for somewhere in the middle.  Note that the default value used by MiGz is 9 (best compression), not -1.
   *
   * This value may be changed at any time.  If changed after writing has begun, it may effect some or all data already
   * written to the stream since that data may not have been compressed yet.
   *
   * @param compressionLevel a compression level ranging from -1 to 9.
   * @return this instance
   */
  public MiGzOutputStream setCompressionLevel(int compressionLevel) {
    if (compressionLevel < -1 || compressionLevel > Deflater.BEST_COMPRESSION) {
      throw new IllegalArgumentException("Compression level must be between -1 and 9, inclusive.");
    }
    _compressionLevel = compressionLevel;
    return this;
  }

  @Override
  public void write(int b) throws IOException {
    _minibuffer[0] = (byte) b;
    write(_minibuffer, 0, 1);
  }

  private long scheduleCurrentBlock(boolean andFlush) throws InterruptedException {
    final long block = _currentBlock++;
    final byte[] buff = _currentBuffer;
    final int length = _currentBufferOffset;
    _threadPool.submit(() -> compress(block, buff, length, andFlush));

    _currentBufferOffset = 0;
    _currentBuffer = _writeBufferPool.take();

    return block;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (_threadPool.isShutdown()) {
      throw new IOException("Stream already closed");
    }

    MiGzUtil.checkException(_exception);

    try {
      while (len > 0) {
        if (len >= _blockSize - _currentBufferOffset) {
          // this fills up our current buffer completely
          int toCopy = _blockSize - _currentBufferOffset;
          System.arraycopy(b, off, _currentBuffer, _currentBufferOffset, toCopy);
          off += toCopy;
          len -= toCopy;
          _currentBufferOffset = _blockSize;

          scheduleCurrentBlock(false);
        } else {
          System.arraycopy(b, off, _currentBuffer, _currentBufferOffset, len);
          _currentBufferOffset += len;
          return; // everything copied
        }
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
  }

  private static void copyIntToLSBByteArray(int value, byte[] target, int offset) {
    target[offset] = (byte) value;
    target[offset + 1] = (byte) (value >> 8);
    target[offset + 2] = (byte) (value >> 16);
    target[offset + 3] = (byte) (value >> 24);
  }

  private void compress(long blockIndex, byte[] block, int blockSize, boolean andFlush) {
    try {
      compressChecked(blockIndex, block, blockSize, andFlush);
    } catch (InterruptedException e) {
      _exception.compareAndSet(null, new UncheckedInterruptedException(e));
    } catch (IOException e) {
      _exception.compareAndSet(null, new UncheckedIOException(e));
    } catch (RuntimeException e) {
      _exception.compareAndSet(null, e);
    }
  }

  private void compressChecked(long blockIndex, byte[] block, int blockSize, boolean andFlush)
      throws InterruptedException, IOException {

    if (blockSize == 0) {
      _writeBufferPool.put(block);
      // this should only happen when flushing
      _outputEpoch.get(blockIndex);
      assert andFlush;
      synchronized (_outputStreamLock) {
        _outputStream.flush();
      }
      _outputEpoch.set(blockIndex + 1);

      return;
    }

    // calculate the CRC
    CRC32 crc32 = new CRC32();
    crc32.reset();
    crc32.update(block, 0, blockSize);

    // setup compressor
    Deflater deflater = new Deflater(_compressionLevel, true);
    deflater.setInput(block, 0, blockSize);
    deflater.finish();

    // the _compressedBuffer already contains the header, compressed data, and footer
    byte[] compressedBuffer = _compressedBuffer.get();

    // do compression
    int deflatedSoFar = 0;
    int deflated;

    while ((deflated = deflater.deflate(compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar,
        compressedBuffer.length - deflatedSoFar - MiGzUtil.GZIP_HEADER_SIZE - MiGzUtil.GZIP_FOOTER_SIZE)) > 0) {
      deflatedSoFar += deflated;
    }

    // return the block to the buffer pool ASAP
    _writeBufferPool.put(block);


    // check the deflater state
    if (!deflater.finished()) {
      throw new IllegalStateException(
          "Data could not be compressed into the expected size; this should not be possible");
    }

    deflater.end();

    // copy over the compressed size to the end of the header
    copyIntToLSBByteArray(deflatedSoFar, compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE - 4);

    // copy in the CRC and uncompressed _length
    copyIntToLSBByteArray((int) crc32.getValue(), compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar);
    copyIntToLSBByteArray(blockSize, compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar + 4);

    // wait for our turn to write to the underlying stream, then do so
    _outputEpoch.get(blockIndex);
    synchronized (_outputStreamLock) {
      _outputStream.write(compressedBuffer, 0, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar + MiGzUtil.GZIP_FOOTER_SIZE);

      if (andFlush) {
        _outputStream.flush();
      }
    }
    _outputEpoch.set(blockIndex + 1);
  }

  /**
   * Writes all data previously written to the underlying stream.  This method may (and probably will) create blocks
   * smaller than the requested block size which will adversely affect the compression ratio, so it's a good idea to
   * call it infrequently, if ever.
   *
   * Blocks until all queued data has been compressed and written.
   *
   * @throws IOException if something goes wrong writing the data
   */
  @Override
  public void flush() throws IOException {
    if (_threadPool.isShutdown()) {
      throw new IOException("Stream already closed");
    }

    MiGzUtil.checkException(_exception);
    try {
      _outputEpoch.get(scheduleCurrentBlock(true) + 1);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (_exception.get() != null) {
        // uh oh, something has gone wrong
        _threadPool.shutdownNow();

        // it's important to report this to the client; otherwise they might think all their data has been written when
        // it actually has not!
        MiGzUtil.checkException(_exception);
      }

      if (_threadPool.isShutdown()) {
        return; // already closed
      }

      if (_currentBufferOffset > 0) {
        try {
          scheduleCurrentBlock(false);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new UncheckedInterruptedException(e);
        }
      }

      _threadPool.shutdown();

      try {
        if (!_threadPool.awaitTermination(3650, TimeUnit.DAYS)) { // wait a long, long time
          throw new IOException("Output thread pool failed to terminate; some data may have failed to be written.");
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException(e.getMessage());
      }
    } finally {
      // always try to close the underlying stream
      synchronized (_outputStreamLock) {
        _outputStream.close();
      }
    }
  }
}
