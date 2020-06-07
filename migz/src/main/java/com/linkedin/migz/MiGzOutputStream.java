/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import com.concurrentli.ExclusiveIdempotentMethod;
import com.concurrentli.ResettableEvent;
import com.concurrentli.SequentialQueue;
import com.concurrentli.UncheckedInterruptedException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveAction;
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
 * "records" in serial as per the GZip specification.  This is perhaps not a widely known feature, but it's why you can
 * concatenate two gzip'ed files and the result will decompress to the concatenation of the original data;
 * MiGz-compressed streams can also be concatenated together and read back by MiGzInputStream to recover the original
 * concatenated data.
 *
 * With the default block size, there is experimentally a very small (~1%) increase in the size of the resulting
 * compressed data when using MiGz vs. traditional GZip, mostly as a consequence of compressing data in chunks rather
 * than holistically, and slightly due to the cost of writing extra GZip record headers (these are very small).  The
 * default block size is chosen to keep the total compressed size extremely close to normal GZip at maximum compression
 * and still allow a high degree of parallelization.
 *
 * The memory requirements of a {@link MiGzOutputStream} scales with the number of threads; the total memory requirement
 * is roughly: {@code (3 + targetThreadCount + actualThreadCount) * blockSize} (actualThreadCount may diverge from the
 * targetThreadCount either because the latter may diverge from the true number of threads in the {@link ForkJoinPool}).
 * On modern hardware, however, this is usually unimportant unless you have a very large number of streams being used
 * concurrently: even with 128 logical cores, at the default block size of 512KB the total memory requirement is still
 * "only" ~130MB, which is probably not significant on a machine with 128 cores!
 */
public class MiGzOutputStream extends OutputStream {
  // The class operates as follows:
  // (1) Writes go into a buffer; these buffers are reused via a buffer pool.
  //     Writes will wait when the buffer pool is (temporarily) exhausted.
  // (2) When a block's worth of data is available, compression is scheduled in the thread pool.
  // (3) A worker thread (CompressionTask) compresses the block and queues it for writing to the underlying stream.
  // (4) One of the worker threads will ultimately write the compressed block to the underlying stream.

  /**
   * The default block size used by MiGz is 512KB.
   */
  public static final int DEFAULT_BLOCK_SIZE = MiGzUtil.DEFAULT_BLOCK_SIZE;

  /**
   * The default level of compression used by MiGz (maximum compression).
   */
  public static final int DEFAULT_COMPRESSION_LEVEL = Deflater.BEST_COMPRESSION;

  // It's probably beneficial to always have some tasks pending; this allows execution to continue before the "main"
  // thread unparks (if it was stalled because there were no free buffers).  The downside is that this consumes
  // additional memory.
  private static final int PENDING_TASKS_TARGET = 2;

  private long _currentBlock = 0; // the index of the block currently being written by calls to write(...)
  private AtomicReference<RuntimeException> _exception = new AtomicReference<>(null); // stores worker exceptions

  private byte[] _currentBuffer; // the current buffer being filled by calls to write(...)
  private int _currentBufferOffset = 0; // offset in _currentBuffer at which additional bytes should be written

  private boolean _closed = false; // remember if we've already close()'d

  private int _compressionLevel = DEFAULT_COMPRESSION_LEVEL; // [-1, 9] value specifying the Deflater compression level

  private final LinkedBlockingQueue<byte[]> _bufferPool; // reuse byte arrays to avoid creation/GC overhead

  // only one Deflater is needed per thread; ideally there should be a way for us to call Deflater::end() when a
  // deflater is no longer required, but, e.g. using a "registry" of instances would run the risk of accruing excessive
  // registry entries over time.  Instead, trust/hope the GC eventually finalizes the deflater instances.
  private final ThreadLocal<ThreadState> _threadLocalState =
      ThreadLocal.withInitial(() -> new ThreadState(createBuffer()));

  private final SequentialQueue<WriteBuffer> _writeQueue; // ordered list of buffers to write to the underlying stream
  private final ExclusiveIdempotentMethod _exclusiveWriter; // allow only single-threaded writing to the stream

  private final int _blockSize; // the number of bytes in the original, uncompressed blocks
  private final int _threadCount; // number of threads used to write the original MiGzipped file

  private final OutputStream _outputStream; // final destination of compressed bytes

  private final ForkJoinPool _threadPool; // compressor threads
  private final boolean _shouldShutdownThreadPool; // true if we are responsible for pool shutdown on close()

  private final int _maxCompressedSize; // what's the maximum, worst-case number of bytes a compressed block can occupy?
  private final byte[] _minibuffer = new byte[1]; // used for single-byte writes

  /**
   * Used to wrap a "working buffer" as well as Deflater instances so they can be made available as thread-locals.
   * Deflaters provide no mechanism for retrieving their level once constructed, so we need to store it ourselves.
   */
  private static class ThreadState {
    byte[] _buffer; // note: not final; we will swap this out frequently
    Deflater _deflater; // may be rarely replaced
    int _level;

    /**
     * Creates an instance that does not hold a deflater (useful as a placeholder).
     */
    ThreadState(byte[] buffer) {
      _buffer = buffer;
      _level = Integer.MIN_VALUE;
    }

    /**
     * Ensures that this instance holds a ready-to-go deflater of the specified compression level
     * @param level the required level
     */
    void ensureDeflateLevelAndReset(int level) {
      if (_level != level) {
        _level = level;
        if (_deflater != null) {
          _deflater.end();
        }
        _deflater = new Deflater(level, true);
      } else {
        _deflater.reset();
      }
    }
  }

  /**
   * Instructs the worker writing a block to the underlying stream what to do after the block is written.
   */
  private enum StreamDirective {
    /**
     * Do nothing.
     */
    NONE,

    /**
     * Flush the stream.
     */
    FLUSH,

    /**
     * Close the stream.
     */
    CLOSE
  }

  /**
   * Parameters for how we want a compressed block to be written.
   */
  private static class WriteOptions {
    private final StreamDirective _directive; // what do do with the stream after writing the compressed block
    private final ResettableEvent _signal; // signal (usually null) to notify anyone waiting for the block to be written

    private WriteOptions(StreamDirective directive, ResettableEvent signal) {
      _directive = directive;
      _signal = signal;
    }
  }

  /**
   * Extends a {@link MiGzBuffer} to add {@link WriteOptions} that specify its dispensation relative to the underlying
   * stream.
   */
  private static class WriteBuffer extends MiGzBuffer {
    private final WriteOptions _writeOptions;
    WriteBuffer(byte[] data, int length, WriteOptions writeOptions) {
      super(data, length);
      _writeOptions = writeOptions;
    }
  }

  /**
   * Creates a new MiGzOutputStream that will output MiGz-compressed bytes to the specified underlying
   * outputStream using the default block size.  Worker tasks will execute on the current {@link ForkJoinPool} returned
   * by {@link ForkJoinTask#getPool()} if applicable, or the {@link ForkJoinPool#commonPool()} otherwise.
   *
   * @param outputStream the stream to which compressed bytes will be written
   */
  public MiGzOutputStream(OutputStream outputStream) {
    this(outputStream, ForkJoinTask.inForkJoinPool() ? ForkJoinTask.getPool() : ForkJoinPool.commonPool(),
        DEFAULT_BLOCK_SIZE);
  }

  /**
   * Creates a new MiGzOutputStream that will output MiGz-compressed bytes to the specified underlying
   * outputStream using a <strong>new</strong> thread pool.
   *
   * @param outputStream the stream to which compressed bytes will be written
   * @param threads the number of threads to use for compression
   * @param blockSize the number of bytes that comprise each block to be compressed; larger blocks result in better
   *                  compression at the expense of more RAM usage when compressing and decompressing.
   */
  public MiGzOutputStream(OutputStream outputStream, int threads, int blockSize) {
    this(outputStream, new ForkJoinPool(threads), blockSize, true);
  }

  /**
   * Creates a new MiGzOutputStream that will output MiGz-compressed bytes to the specified underlying outputStream.
   *
   * @param outputStream the stream to which compressed bytes will be written
   * @param threadPool the {@link ForkJoinPool} where compression worker tasks will be executed
   * @param blockSize the number of bytes that comprise each block to be compressed; larger blocks result in better
   *                  compression at the expense of more RAM usage when compressing and decompressing.
   */
  public MiGzOutputStream(OutputStream outputStream, ForkJoinPool threadPool, int blockSize) {
    this(outputStream, threadPool, blockSize, false);
  }

  /**
   * Creates a new MiGzOutputStream that will output MiGz-compressed bytes to the specified underlying outputStream.
   *
   * @param outputStream the stream to which compressed bytes will be written
   * @param threadPool the {@link ForkJoinPool} where compression worker tasks will be executed
   * @param blockSize the number of bytes that comprise each block to be compressed; larger blocks result in better
   *                  compression at the expense of more RAM usage when compressing and decompressing.
   * @param shouldShutdownThreadPool true if this instance "owns" the thread pool and should shut it down when close()
   *                                 is called
   */
  private MiGzOutputStream(OutputStream outputStream, ForkJoinPool threadPool, int blockSize,
      boolean shouldShutdownThreadPool) {
    _outputStream = outputStream;
    _maxCompressedSize = MiGzUtil.maxCompressedSize(blockSize);
    _threadCount = threadPool.getParallelism();
    _blockSize = blockSize;

    // calculate the total number of buffers in the pool, including the starts-as-checked-out _currentBuffer
    int bufferPoolSize = PENDING_TASKS_TARGET + _threadCount + 1;

    // the number of pending writes is limited by the number of buffers in the pool:
    _writeQueue = new SequentialQueue<>(bufferPoolSize + 1); // writes to this queue should never block
    _exclusiveWriter = new ExclusiveIdempotentMethod(this::writeToUnderlyingStream);

    _threadPool = threadPool;
    _shouldShutdownThreadPool = shouldShutdownThreadPool;

    // create pooled buffers
    _currentBuffer = createBuffer();
    _bufferPool = new LinkedBlockingQueue<>(
        IntStream.range(0, bufferPoolSize - 1).mapToObj(i -> createBuffer()).collect(Collectors.toList()));
  }

  private byte[] createBuffer() {
    byte[] res =
        new byte[MiGzUtil.GZIP_HEADER_SIZE + _maxCompressedSize + MiGzUtil.GZIP_FOOTER_SIZE];
    return res;
  }

  private static void addGZipHeader(byte[] buffer) {
    System.arraycopy(MiGzUtil.GZIP_HEADER, 0, buffer, 0, MiGzUtil.GZIP_HEADER.length);
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

  private void scheduleCurrentBlock(WriteOptions writeOptions) throws InterruptedException {
    final long block = _currentBlock++;
    final byte[] buff = _currentBuffer;
    final int length = _currentBufferOffset;

    _threadPool.execute(new CompressTask(block, buff, length, writeOptions));

    _currentBufferOffset = 0;
    _currentBuffer = _bufferPool.take(); // will block if the pool of buffers is exhausted
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (_closed) {
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

          scheduleCurrentBlock(new WriteOptions(StreamDirective.NONE, null));
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

  /**
   * A ForkJoinTask that compresses a provided block into a provided buffer with the provided options and then provides
   * for it to be written to the underlying stream.  There is much provision.
   */
  private class CompressTask extends RecursiveAction {
    final long _blockIndex;
    final byte[] _data;
    final int _dataLength;
    final WriteOptions _writeOptions;

    /**
     * Creates a new taks that will compress a block of data and enqueue it for writing to the underlying stream.
     *
     * @param blockIndex the index of the block (used to order the writes to the underlying stream)
     * @param data the block of data to compressed
     * @param dataLength the number of bytes in {@code data} that contain the data to be compressed
     * @param writeOptions specify what happens immediately after the compressed data block is written to the underlying
     *                     stream
     */
    CompressTask(long blockIndex, byte[] data, int dataLength, WriteOptions writeOptions) {
      _blockIndex = blockIndex;
      _data = data;
      _dataLength = dataLength;
      _writeOptions = writeOptions;
    }

    public void compute() {
      try {
        compressChecked();
      } catch (InterruptedException e) {
        _exception.compareAndSet(null, new UncheckedInterruptedException(e));
      } catch (RuntimeException e) {
        _exception.compareAndSet(null, e);
      }
    }

    private void compressChecked() throws InterruptedException {
      if (_dataLength == 0) {
        assert _writeOptions._directive != StreamDirective.NONE; // this should only happen when flushing or closing
        _writeQueue.enqueue(_blockIndex, new WriteBuffer(_data, 0, _writeOptions));

        // try to write from the queue, if nobody else is already writing
        _exclusiveWriter.tryRun();

        return;
      }

      // calculate the CRC
      CRC32 crc32 = new CRC32();
      crc32.reset();
      crc32.update(_data, 0, this._dataLength);

      // Grab remaining resources needed for compression
      ThreadState threadState =  _threadLocalState.get();
      threadState.ensureDeflateLevelAndReset(_compressionLevel);

      Deflater deflater = threadState._deflater;
      deflater.setInput(_data, 0, this._dataLength);
      deflater.finish();

      byte[] compressedBuffer = threadState._buffer;

      addGZipHeader(compressedBuffer);

      // do compression
      int deflatedSoFar = 0;
      int deflated;

      while ((deflated = deflater.deflate(compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar,
          compressedBuffer.length - deflatedSoFar - MiGzUtil.GZIP_HEADER_SIZE - MiGzUtil.GZIP_FOOTER_SIZE)) > 0) {
        deflatedSoFar += deflated;
      }

      // we don't need _data anymore, but we want to write compressedBuffer.  Replace our ThreadState's buffer with
      // _data so that we can pass compressedBuffer to the write queue.
      threadState._buffer = _data; // implicitly detaches compressedBuffer from this thread

      // check the deflater state
      if (!deflater.finished()) {
        throw new IllegalStateException(
            "Data could not be compressed into the expected size; this should not be possible");
      }

      // copy over the compressed size to the end of the header
      copyIntToLSBByteArray(deflatedSoFar, compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE - 4);

      // copy in the CRC and uncompressed _length (the footer)
      copyIntToLSBByteArray((int) crc32.getValue(), compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar);
      copyIntToLSBByteArray(this._dataLength, compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar + 4);

      // put our block in the write queue
      _writeQueue.enqueue(_blockIndex,
          new WriteBuffer(compressedBuffer, MiGzUtil.GZIP_HEADER_SIZE + deflatedSoFar + MiGzUtil.GZIP_FOOTER_SIZE,
              _writeOptions));

      // try to write from the queue, if nobody else is already writing
      _exclusiveWriter.tryRun();
    }
  }

  /**
   * Writes any available blocks from the write queue.
   *
   * At most one thread will execute this method at a given time.
   */
  private void writeToUnderlyingStream() {
    try {
      // use a managed block so the thread pool can create an extra thread while we block on writes
      ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
        private boolean _cachedHasNextAvailable;

        @Override
        public boolean block() throws InterruptedException {
          while (_cachedHasNextAvailable || _writeQueue.isNextAvailable()) {
            _cachedHasNextAvailable = false;

            final WriteBuffer buffer = _writeQueue.dequeue();
            try {
              _outputStream.write(buffer.getData(), 0, buffer.getLength());

              switch (buffer._writeOptions._directive) {
                case CLOSE:
                  _outputStream.close();
                  break;
                case FLUSH:
                  _outputStream.flush();
                  break;
              }

              // signal anyone waiting for this block to complete
              if (buffer._writeOptions._signal != null) {
                buffer._writeOptions._signal.set();
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            } finally {
              _bufferPool.put(buffer.getData()); // return buffer to pool
            }
          }

          return true;
        }

        @Override
        public boolean isReleasable() {
          return !(_cachedHasNextAvailable = _writeQueue.isNextAvailable());
        }
      });
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e);
    }
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
    if (_closed) {
      throw new IOException("Stream already closed");
    }
    MiGzUtil.checkException(_exception);

    ResettableEvent signal = new ResettableEvent(false);
    try {
      scheduleCurrentBlock(new WriteOptions(StreamDirective.FLUSH, signal));
      signal.getWithoutReset(); // wait for the block to be written and flushed
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (_closed) {
      return;
    }

    MiGzUtil.checkException(_exception); // make sure there are no outstanding exceptions

    _closed = true;

    try {
      ResettableEvent signal = new ResettableEvent(false);
      scheduleCurrentBlock(new WriteOptions(StreamDirective.CLOSE, signal));
      signal.getWithoutReset(); // wait for all data to be written and all resources released
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e);
    }

    MiGzUtil.checkException(_exception); // there may have been exceptions while we were closing

    if (_shouldShutdownThreadPool) {
      _threadPool.shutdown();
    }
  }
}
