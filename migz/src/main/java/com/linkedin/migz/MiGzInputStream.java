/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import com.concurrentli.ManagedDequeueBlocker;
import com.concurrentli.ManagedStreamReadBlocker;
import com.concurrentli.SequentialQueue;
import com.concurrentli.UncheckedInterruptedException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


/**
 * MiGzInputStream reads compressed data from an underlying stream that was previously compressed by MiGz,
 * parallelizing the decompression for maximum speed.
 *
 * Please note that while MiGz-compressed streams can be read by any gzip utility or library, MiGzInputStream
 * ONLY reads MiGz-compressed data and cannot read gzip'ed data from other sources.
 *
 * MiGz breaks the data into blocks, compresses each block in parallel, and then writes them as them multiple gzip
 * "records" in serial as per the GZip specification (this is perhaps not a widely known feature, but it's why you can
 * concatenate two gzip'ed files and the result will decompress to the concatenation of the original data).
 *
 * MiGz-compressed streams can be concatenated together and read back by MiGzInputStream to recover the original
 * concatenated data.
 *
 * With the default block size, there is experimentally a very small (~1%) increase in the compressed data size penalty
 * for MiGz vs. standard GZip, mostly as a consequence of compressing data in chunks rather than holistically, and
 * slightly due to the cost of writing extra GZip record headers (these are very small).  The default block size is
 * chosen to keep the total compressed size extremely close to normal GZip at maximum compression and still allow a high
 * degree of parallelization.
 */
public class MiGzInputStream extends InputStream {
  /**
   * The class operates as follows:
   * (1) Threads in the thread pool read blocks of compressed data from the underlying stream.
   * (2) Each thread decompresses its block into an output buffer taken from the buffer pool.
   * (3) The output buffer is placed into the outputBufferQueue for the client to read(...)
   */
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private long _currentBlock = 0; // the current block being read from the input stream
  private volatile boolean _eosCompressed = false; // have reached the end of compressed data?
  private boolean _eosDecompressed = false; // have we reached the end of decompressed data?

  private MiGzBuffer _activeDecompressedBuffer = null; // current buffer being read(...) or readBuffer()'ed
  private int _activeDecompressedBufferOffset = 0; // offset of the next byte to be read within that buffer

  private final List<TaskState> _taskStatePopulation; // needed for closing
  private final LinkedBlockingQueue<TaskState> _taskStatePool; // pool of buffers for decompressed bytes
  private final LinkedBlockingQueue<byte[]> _decompressedBufferPool; // pool of buffers for decompressed bytes
  // sequential queue of decompressed data ready to be read by the client via readBuffer() or read(...):
  private final SequentialQueue<MiGzBuffer> _decompressedBufferQueue;
  // number of threads used to write the original MiGzipped file
  private final InputStream _inputStream; // source of compressed bytes
  private final ConcurrentHashMap<DecompressTask, Boolean> _activeTasks;
  private final boolean _ownsThreadPool; // determines whether the thread pool should be terminated on close()
  private final ForkJoinPool _threadPool; // decompressor thread pool
  //private final ForkJoinTask[] _tasks; // keep track of these so we can cancel them later if needed
  private final byte[] _minibuff = new byte[1]; // used for single-byte read()s

  /**
   * Creates a new MiGzInputStream that will read MiGz-compressed bytes from the specified underlying
   * inputStream using the default number of threads.  Worker tasks will execute on the current {@link ForkJoinPool}
   * returned by {@link ForkJoinTask#getPool()} if applicable, the {@link ForkJoinPool#commonPool()} otherwise,
   * with a maximum number of concurrent workers equal to the target parallelism of the pool.
   *
   * @param inputStream the stream from which compressed bytes will be read
   * @throws UncheckedIOException if a problem occurs reading the block size header
   */
  public MiGzInputStream(InputStream inputStream) {
    this(inputStream, ForkJoinTask.inForkJoinPool() ?  ForkJoinTask.getPool() : ForkJoinPool.commonPool());
  }

  /**
   * Creates a new MiGzInputStream that will read MiGz-compressed bytes from the specified underlying
   * inputStream.  The number of worker threads will be equal to the parallelism of the pool.
   *
   * @param inputStream the stream from which compressed bytes will be read
   * @param threadPool the thread pool on which worker threads will be scheduled
   */
  public MiGzInputStream(InputStream inputStream, ForkJoinPool threadPool) {
    this(inputStream, threadPool, threadPool.getParallelism());
  }

  /**
   * Creates a new MiGzInputStream that will read MiGz-compressed bytes from the specified underlying
   * inputStream.  A <strong>new</strong> ForkJoinPool will be created to accommodate the specified number of threads.
   *
   * @param inputStream the stream from which compressed bytes will be read
   * @param threadCount the maximum number of threads to use for decompression
   */
  public MiGzInputStream(InputStream inputStream, int threadCount) {
    this(inputStream, new ForkJoinPool(threadCount), threadCount, true);
  }

  /**
   * Creates a new MiGzInputStream that will read MiGz-compressed bytes from the specified underlying
   * inputStream.
   *
   * @param inputStream the stream from which compressed bytes will be read
   * @param threadPool the thread pool on which worker threads will be scheduled
   * @param threadCount the maximum number of threads to use for decompression; limited by the number of threads in the
   *                provided {@code threadPool}
   */
  public MiGzInputStream(InputStream inputStream, ForkJoinPool threadPool, int threadCount) {
    this(inputStream, threadPool, threadCount, false);
  }

  /**
   * Creates a new MiGzInputStream that will read MiGz-compressed bytes from the specified underlying
   * inputStream.
   *
   * @param inputStream the stream from which compressed bytes will be read
   * @param threadPool the thread pool on which worker threads will be scheduled
   * @param threadCount the maximum number of threads to use for decompression; limited by the number of threads in the
   *                provided {@code threadPool}
   * @param ownsThreadPool whether this instance "owns" the thread pool and should shut it down when close() is called
   */
  private MiGzInputStream(InputStream inputStream, ForkJoinPool threadPool, int threadCount, boolean ownsThreadPool) {
    _inputStream = inputStream;
    _threadPool = threadPool;
    _ownsThreadPool = ownsThreadPool;
    int outputBufferCount = 2 * threadCount;

    _activeTasks = new ConcurrentHashMap<>(threadCount);
    _taskStatePopulation = IntStream.range(0, threadCount).mapToObj(i -> new TaskState()).collect(Collectors.toList());
    _taskStatePool = new LinkedBlockingQueue<>(_taskStatePopulation);

    _decompressedBufferPool = new LinkedBlockingQueue<>(Collections.nCopies(outputBufferCount, EMPTY_BYTE_ARRAY));

    // making the queue larger than the number of output buffers guarantees that an enqueue call on SequentialQueue will
    // not block, since the indices written to are assigned sequentially only after the worker thread obtains a buffer
    _decompressedBufferQueue = new SequentialQueue<>(outputBufferCount + 1);

    _threadPool.execute(new DecompressTask());
  }

  private void enqueueException(long blockIndex, RuntimeException e) throws InterruptedException {
    _eosCompressed = true; // try to stop any further reads from underlying input stream
    _decompressedBufferQueue.enqueueException(blockIndex, e); // throw an exception when this block is read(...)
  }

  private static class TaskState {
    byte[] _buffer = new byte[MiGzUtil.maxCompressedSize(MiGzUtil.DEFAULT_BLOCK_SIZE) + MiGzUtil.GZIP_FOOTER_SIZE];
    final byte[] _headerBuffer = new byte[MiGzUtil.GZIP_HEADER_SIZE];
    Inflater _inflater = new Inflater(true);

    void close() {
      _inflater.end();
    }
  }

  private static int readFromStream(InputStream inputStream, byte[] buffer, int count) throws IOException {
    int read;
    int offset = 0;
    while ((read = inputStream.read(buffer, offset, count - offset)) > 0) {
      offset += read;
    }
    return offset;
  }

  private class DecompressTask extends RecursiveAction {
    private Thread _executionThread;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (mayInterruptIfRunning) {
        synchronized (this) {
          if (_executionThread != null) {
            _executionThread.interrupt();
          }
        }
      }
      return super.cancel(mayInterruptIfRunning);
    }

    @Override
    protected void compute() {
      _activeTasks.put(this, true);
      synchronized (this) { // this won't block unless we're closing (and even then, almost certainly not...)
        _executionThread = Thread.currentThread();
      }
      try {
        try {
          decompressorThread();
        } finally { // clear our execution thread before handling any exceptions or clearing ourselves from activeTasks
          synchronized (this) {
            _executionThread = null;
          }
        }
      } catch (InterruptedException e) {
        throw new UncheckedInterruptedException(e);
      } finally {
        _activeTasks.remove(this);
      }
    }

    private void decompressorThread() throws InterruptedException {
      TaskState taskState = ManagedDequeueBlocker.dequeue(_taskStatePool);

      byte[] headerBuffer = taskState._headerBuffer;
      byte[] buffer = taskState._buffer;
      Inflater inflater = taskState._inflater;

      byte[] outputBuffer = ManagedDequeueBlocker.dequeue(_decompressedBufferPool);

      // check if we should stop
      if (_eosCompressed) {
        return;
      }

      long myBlock = -1; // need to initialize this so Java doesn't complain that an uninitialized value is read in the
      // catch {...} blocks, but in practice it will always be set to the correct value if/when those
      // blocks execute.
      try {
        final int compressedSize;
        boolean ensureBufferCapacity = false;

        myBlock = _currentBlock++;

        // first read the header
        int headerBufferBytesRead = ManagedStreamReadBlocker.read(_inputStream, headerBuffer, headerBuffer.length);

        // eos or partially missing header?
        if (headerBufferBytesRead < headerBuffer.length) {
          _eosCompressed = true;
          _decompressedBufferQueue.enqueue(myBlock, null); // never blocks

          if (headerBufferBytesRead == 0) {
            // nothing more to read
            return;
          } else {
            // something more to read, but it's not long enough to be a valid header
            throw new IOException("File is not MiGz formatted");
          }
        }

        compressedSize = getIntFromLSBByteArray(headerBuffer, headerBuffer.length - 4);
        int toRead = compressedSize + MiGzUtil.GZIP_FOOTER_SIZE;

        // get new, bigger buffer if necessary
        if (buffer.length < toRead) {
          buffer = new byte[toRead];
          ensureBufferCapacity = true;
        }

        ManagedStreamReadBlocker.read(_inputStream, buffer, toRead);

        // note that forking synchronizes our child thread with our current state
        new DecompressTask().fork(); // we've finished using exclusive resources, let someone else run

        int putativeInflatedSize = getIntFromLSBByteArray(buffer, compressedSize + 4);

        // check if the compressed buffer is large enough for all future inputs with this inflated block size;
        // this avoids re-allocating the buffer multiple times above, though this never happens when the default block
        // size is used:
        if (ensureBufferCapacity && buffer.length < MiGzUtil.maxCompressedSize(putativeInflatedSize) + MiGzUtil.GZIP_FOOTER_SIZE) {
          buffer = new byte[MiGzUtil.maxCompressedSize(putativeInflatedSize) + MiGzUtil.GZIP_FOOTER_SIZE];
        }

        // enlarge the output buffer if necessary
        if (outputBuffer.length < putativeInflatedSize) {
          outputBuffer = new byte[putativeInflatedSize];
        }

        inflater.reset();
        inflater.setInput(buffer, 0, compressedSize);
        int uncompressedSize = inflater.inflate(outputBuffer);

        // This is extremely basic error checking: just check the decompressed size;
        // at greater CPU cost we could also check the CRC32 and the header, too
        if (uncompressedSize != putativeInflatedSize) {
          throw new IOException("The number of bytes actually decompressed bytes does not match the number of " + "uncompressed bytes recorded in the GZip record");
        } else if (!inflater.finished()) {
          throw new IOException("The decompressed size is larger than that claimed in the GZip record");
        }

        _taskStatePool.put(taskState); // return resources to pool; never blocks
        _decompressedBufferQueue.enqueue(myBlock, new MiGzBuffer(outputBuffer, uncompressedSize)); // never blocks
      } catch (IOException e) {
        enqueueException(myBlock, new UncheckedIOException(e));
        return;
      } catch (DataFormatException e) {
        enqueueException(myBlock, new RuntimeException(e));
        return;
      } catch (RuntimeException e) {
        enqueueException(myBlock, e);
        return;
      }
    }
  }

  private static int getIntFromLSBByteArray(byte[] source, int offset) {
    return
        Byte.toUnsignedInt(source[offset])
        | (Byte.toUnsignedInt(source[offset + 1]) << 8)
        | (Byte.toUnsignedInt(source[offset + 2]) << 16)
        | (Byte.toUnsignedInt(source[offset + 3]) << 24);
  }

  @Override
  public int read() throws IOException {
    if (read(_minibuff, 0, 1) < 1) {
      return -1;
    } else {
      return Byte.toUnsignedInt(_minibuff[0]);
    }
  }

  /**
   * {@link MiGzInputStream} provides this alternative to the read(...) method to avoid an extra array copy where it is
   * not necessary.  Returns the next MiGzBuffer object containing a byte array of decompressed data and the length of
   * the data in the byte array (the decompressed data always starts at offset 0), or null if the end of stream has been
   * reached.
   *
   * The buffer remains valid until the next call to {@link #readBuffer()} or one of the {@code read(...)} methods;
   * after that, MiGz may modify its contents to store further decompressed data.
   *
   * You should avoid interleaving {@link #readBuffer()} and {@code read(...)} calls, as this can severely harm
   * performance.  Use one method or the other, not both, to read the stream.
   *
   * @return a Buffer containing decompressed data, or null if the end of stream has been reached
   */
  public MiGzBuffer readBuffer() {
    if (!ensureBuffer()) {
      return null; // eos
    }

    if (_activeDecompressedBufferOffset > 0) {
      // client is interleaving read(...) and readBuffer() calls--this is not advised!

      _activeDecompressedBuffer = new MiGzBuffer(_activeDecompressedBuffer.getData(),
          _activeDecompressedBuffer.getLength() - _activeDecompressedBufferOffset);

      System.arraycopy(_activeDecompressedBuffer.getData(), _activeDecompressedBufferOffset,
          _activeDecompressedBuffer.getData(), 0, _activeDecompressedBuffer.getLength());
    }

    _activeDecompressedBufferOffset = _activeDecompressedBuffer.getLength(); // we've now read everything in the buffer
    return _activeDecompressedBuffer;
  }

  // gets the next read buffer if needed, returning true if there is more data to read, false if end-of-stream
  private boolean ensureBuffer() {
    if (_activeDecompressedBuffer == null || _activeDecompressedBuffer.getLength() == _activeDecompressedBufferOffset) {
      try {
        if (_activeDecompressedBuffer != null) {
          // put our current buffer back in the pool
          _decompressedBufferPool.offer(_activeDecompressedBuffer.getData());
        } else if (_eosDecompressed) {
          return false;
        }

        _activeDecompressedBuffer = _decompressedBufferQueue.dequeue();

        if (_activeDecompressedBuffer == null) {
          _eosDecompressed = true;
          return false;
        }

        _activeDecompressedBufferOffset = 0;
      } catch (InterruptedException e) {
        throw new UncheckedInterruptedException(e);
      }
    }

    return true; // not yet the end of stream
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (!ensureBuffer()) {
      return -1; // eos
    }

    int toRead = Math.min(len, _activeDecompressedBuffer.getLength() - _activeDecompressedBufferOffset);
    System.arraycopy(_activeDecompressedBuffer.getData(), _activeDecompressedBufferOffset, b, off, toRead);
    _activeDecompressedBufferOffset += toRead;
    return toRead;
  }

  @Override
  public int available() throws IOException {
    if ((_activeDecompressedBuffer == null || _activeDecompressedBuffer.getLength() == _activeDecompressedBufferOffset)
      && _decompressedBufferQueue.isNextAvailable()) {
      ensureBuffer(); // shouldn't block (at least not more than trivially)
    }

    if (_activeDecompressedBuffer != null) {
      return _activeDecompressedBuffer.getLength() - _activeDecompressedBufferOffset;
    }

    return 0;
  }

  @Override
  public void close() throws IOException {
    _eosCompressed = true; // will (eventually) result in all tasks stopping

    _decompressedBufferPool.offer(EMPTY_BYTE_ARRAY); { } // there might be a thread waiting on a buffer

    // try to cancel/interrupt all tasks, then wait for them to finish
    DecompressTask[] activeTasks = _activeTasks.keySet().toArray(new DecompressTask[0]);
    Arrays.stream(activeTasks).forEach(task -> task.cancel(true));

    try {
      ForkJoinTask.invokeAll(activeTasks); // wait until our tasks have completed; may rethrow worker exceptions
    } catch (Exception e) {
      // do nothing--exceptions are expected here
    } finally {
      _inputStream.close(); // now safe to close underlying stream
      _taskStatePopulation.forEach(TaskState::close); // release task state resources

      try {
        if (_ownsThreadPool) {
          _threadPool.shutdown();
        }
      } catch (Exception ignored) { } // ignore exceptions while shutting down
    }
  }
}
