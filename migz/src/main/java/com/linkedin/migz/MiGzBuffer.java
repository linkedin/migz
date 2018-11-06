/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

/**
 * MiGzBuffer directly exposes MiGz's internal buffers for better performance relative to the typical stream interface.
 */
public final class MiGzBuffer {
  private final byte[] _data;
  private final int _length;

  /**
   * Gets the byte array containing this buffer's bytes.  Bytes [0, getLength()] are valid data.
   *
   * @return the buffer's byte array
   */
  public byte[] getData() {
    return _data;
  }

  /**
   * Gets the length of the data stored in this buffer.  Bytes [0, getLength()] are valid data.
   *
   * @return the buffer's length
   */
  public int getLength() {
    return _length;
  }

  MiGzBuffer(byte[] data, int length) {
    this._data = data;
    this._length = length;
  }
}
