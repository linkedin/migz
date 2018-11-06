/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import com.concurrentli.Interrupted;
import com.concurrentli.SequentialQueue;
import com.concurrentli.UncheckedInterruptedException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.ArrayBlockingQueue;
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
  public static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

  private long _currentBlock = 0; // the current block being read from the input stream
  private volatile boolean _eosCompressed = false; // have reached the end of compressed data?
  private boolean _eosDecompressed = false; // have we reached the end of decompressed data?

  private MiGzBuffer _activeDecompressedBuffer = null; // current buffer being read(...) or readBuffer()'ed
  private int _activeDecompressedBufferOffset = 0; // offset of the next byte to be read within that buffer

  private final ArrayBlockingQueue<byte[]> _decompressedBufferPool; // pool of buffers for decompressed bytes
  // sequential queue of decompressed data ready to be read by the client via readBuffer() or read(...):
  private final SequentialQueue<MiGzBuffer> _decompressedBufferQueue;

  // number of threads used to write the original MiGzipped file
  private final int _threads;

  private final InputStream _inputStream; // source of compressed bytes

  private final Thread[] _threadPool; // decompressor threads
  private final byte[] _minibuff = new byte[1]; // used for single-byte read()s

  /**
   * Creates a new MiGzInputStream that will read MiGz-compressed bytes from the specified underlying
   * inputStream using the default number of threads.
   *
   * @param inputStream the stream from which compressed bytes will be read
   * @throws UncheckedIOException if a problem occurs reading the block size header
   */
  public MiGzInputStream(InputStream inputStream) {
    this(inputStream, DEFAULT_THREAD_COUNT);
  }

  /**
   * Creates a new MiGzInputStream that will read MiGz-compressed bytes from the specified underlying
   * inputStream.
   *
   * @param inputStream the stream from which compressed bytes will be read
   * @param threads the number of threads to use for decompression
   */
  public MiGzInputStream(InputStream inputStream, int threads) {
    _inputStream = inputStream;
    _threads = threads;
    _threadPool = new Thread[_threads];
    int outputBufferCount = 2 * threads;

    _decompressedBufferPool =
        new ArrayBlockingQueue<byte[]>(outputBufferCount, false, IntStream.range(0, outputBufferCount)
          .mapToObj(i -> new byte[MiGzUtil.DEFAULT_BLOCK_SIZE])
          .collect(Collectors.toList()));
    _decompressedBufferQueue = new SequentialQueue<>(outputBufferCount + 1);

    for (int i = 0; i < _threads; i++) {
      _threadPool[i] = new Thread(Interrupted.ignored(this::decompressorThread));
      _threadPool[i].setDaemon(true);
      _threadPool[i].start();
    }
  }

  private void enqueueException(long blockIndex, RuntimeException e) throws InterruptedException {
    _eosCompressed = true; // try to stop any further reads from underlying input stream
    _decompressedBufferQueue.enqueueException(blockIndex, e); // throw an exception when this block is read(...)
  }

  private void decompressorThread() throws InterruptedException {
    Inflater inflater = new Inflater(true);
    try {
      decompressorThreadWithInflater(inflater);
    } finally {
      inflater.end();
    }
  }

  private void decompressorThreadWithInflater(final Inflater inflater)
      throws InterruptedException {

    byte[] buffer =
        new byte[MiGzUtil.maxCompressedSize(MiGzUtil.DEFAULT_BLOCK_SIZE) + MiGzUtil.GZIP_FOOTER_SIZE];

    final byte[] headerBuffer = new byte[MiGzUtil.GZIP_HEADER_SIZE];

    while (true) {
      long myBlock = -1; // need to initialize this so Java doesn't complain that an uninitialized value is read in the
                         // catch {...} blocks, but in practice it will always be set to the correct value if/when those
                         // blocks execute.
      try {
        final int compressedSize;
        boolean ensureBufferCapacity = false;

        // before reading the input, get a place to put the decompressed output:
        byte[] outputBuffer = _decompressedBufferPool.take();

        synchronized (_inputStream) {
          if (_eosCompressed) {
            return;
          }

          myBlock = _currentBlock++;

          // first read the header
          int headerBufferBytesRead = readFromInputStream(headerBuffer, headerBuffer.length);

          // eos or partially missing header?
          if (headerBufferBytesRead < headerBuffer.length) {
            _eosCompressed = true;
            _decompressedBufferQueue.enqueue(myBlock, null);

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

          readFromInputStream(buffer, toRead);
        }

        int putativeInflatedSize = getIntFromLSBByteArray(buffer, compressedSize + 4);

        // check if the compressed buffer is large enough for all future inputs with this inflated block size;
        // this avoids re-allocating the buffer multiple times above, though this never happens when the default block
        // size is used:
        if (ensureBufferCapacity
            && buffer.length < MiGzUtil.maxCompressedSize(putativeInflatedSize) + MiGzUtil.GZIP_FOOTER_SIZE) {
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
          throw new IOException("The number of bytes actually decompressed bytes does not match the number of "
              + "uncompressed bytes recorded in the GZip record");
        } else if (!inflater.finished()) {
          throw new IOException("The decompressed size is larger than that claimed in the GZip record");
        }

        _decompressedBufferQueue.enqueue(myBlock, new MiGzBuffer(outputBuffer, uncompressedSize));
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

  private int readFromInputStream(byte[] buffer, int length) throws IOException {
    int read;
    int offset = 0;

    // first read the header
    while ((read = _inputStream.read(buffer, offset, length - offset)) > 0) {
      offset += read;
    }

    return offset;
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
   * You should avoid interleaving readBuffer() and read(...) calls, as this can severely harm performance.  Use one
   * method or the other, not both, to read the stream.
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
    if (_activeDecompressedBuffer != null) {
      return _activeDecompressedBuffer.getLength() - _activeDecompressedBufferOffset;
    }

    return 0;
  }

  @Override
  public void close() throws IOException {
    for (int i = 0; i < _threadPool.length; i++) {
      _threadPool[i].interrupt();
    }
    _inputStream.close();
  }
}
