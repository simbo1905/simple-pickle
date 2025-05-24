package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;


class ZigZagEncoding {

  /// Writes a long value to the given buffer in LEB128 ZigZag encoded format
  /// @param buffer the buffer to write to
  /// @param value  the value to write to the buffer
  /// @return the number of bytes written
  // FIXME: restore the original methods for putInt and putLong as much cheaper to get the size from the buffer old/new position
  static int putLong(ByteBuffer buffer, long value) {
    int count = 0;
    value = (value << 1) ^ (value >> 63);
    if (value >>> 7 == 0) {
      buffer.put((byte) value);
      count++;
    } else {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      count++;
      if (value >>> 14 == 0) {
        buffer.put((byte) (value >>> 7));
        count++;
      } else {
        buffer.put((byte) (value >>> 7 | 0x80));
        count++;
        if (value >>> 21 == 0) {
          buffer.put((byte) (value >>> 14));
          count++;
        } else {
          buffer.put((byte) (value >>> 14 | 0x80));
          count++;
          if (value >>> 28 == 0) {
            buffer.put((byte) (value >>> 21));
            count++;
          } else {
            buffer.put((byte) (value >>> 21 | 0x80));
            count++;
            if (value >>> 35 == 0) {
              buffer.put((byte) (value >>> 28));
              count++;
            } else {
              buffer.put((byte) (value >>> 28 | 0x80));
              count++;
              if (value >>> 42 == 0) {
                buffer.put((byte) (value >>> 35));
                count++;
              } else {
                buffer.put((byte) (value >>> 35 | 0x80));
                count++;
                if (value >>> 49 == 0) {
                  buffer.put((byte) (value >>> 42));
                  count++;
                } else {
                  buffer.put((byte) (value >>> 42 | 0x80));
                  count++;
                  if (value >>> 56 == 0) {
                    buffer.put((byte) (value >>> 49));
                    count++;
                  } else {
                    buffer.put((byte) (value >>> 49 | 0x80));
                    buffer.put((byte) (value >>> 56));
                    count += 2;
                  }
                }
              }
            }
          }
        }
      }
    }
    return count;
  }

  /// Writes an int value to the given buffer in LEB128-64b9B ZigZag encoded format
  /// @param buffer the buffer to write to
  /// @param value  the value to write to the buffer
  /// @return the number of bytes written
  static int putInt(ByteBuffer buffer, int value) {
    int count = 0;
    value = (value << 1) ^ (value >> 31);
    if (value >>> 7 == 0) {
      buffer.put((byte) value);
      count++;
    } else {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      count++;
      if (value >>> 14 == 0) {
        buffer.put((byte) (value >>> 7));
        count++;
      } else {
        buffer.put((byte) (value >>> 7 | 0x80));
        count++;
        if (value >>> 21 == 0) {
          buffer.put((byte) (value >>> 14));
          count++;
        } else {
          buffer.put((byte) (value >>> 14 | 0x80));
          count++;
          if (value >>> 28 == 0) {
            buffer.put((byte) (value >>> 21));
            count++;
          } else {
            buffer.put((byte) (value >>> 21 | 0x80));
            buffer.put((byte) (value >>> 28));
            count += 2;
          }
        }
      }
    }
    return count;
  }

  /// Read an LEB128-64b9B ZigZag encoded long value from the given buffer
  /// @param buffer the buffer to read from
  /// @return the value read from the buffer
  static long getLong(ByteBuffer buffer) {
    long v = buffer.get();
    long value = v & 0x7F;
    if ((v & 0x80) != 0) {
      v = buffer.get();
      value |= (v & 0x7F) << 7;
      if ((v & 0x80) != 0) {
        v = buffer.get();
        value |= (v & 0x7F) << 14;
        if ((v & 0x80) != 0) {
          v = buffer.get();
          value |= (v & 0x7F) << 21;
          if ((v & 0x80) != 0) {
            v = buffer.get();
            value |= (v & 0x7F) << 28;
            if ((v & 0x80) != 0) {
              v = buffer.get();
              value |= (v & 0x7F) << 35;
              if ((v & 0x80) != 0) {
                v = buffer.get();
                value |= (v & 0x7F) << 42;
                if ((v & 0x80) != 0) {
                  v = buffer.get();
                  value |= (v & 0x7F) << 49;
                  if ((v & 0x80) != 0) {
                    v = buffer.get();
                    value |= v << 56;
                  }
                }
              }
            }
          }
        }
      }
    }
    value = (value >>> 1) ^ (-(value & 1));
    return value;
  }

  /// Read an LEB128-64b9B ZigZag encoded int value from the given buffer
  /// @param buffer the buffer to read from
  /// @return the value read from the buffer
  static int getInt(ByteBuffer buffer) {
    int v = buffer.get();
    int value = v & 0x7F;
    if ((v & 0x80) != 0) {
      v = buffer.get();
      value |= (v & 0x7F) << 7;
      if ((v & 0x80) != 0) {
        v = buffer.get();
        value |= (v & 0x7F) << 14;
        if ((v & 0x80) != 0) {
          v = buffer.get();
          value |= (v & 0x7F) << 21;
          if ((v & 0x80) != 0) {
            v = buffer.get();
            value |= (v & 0x7F) << 28;
          }
        }
      }
    }
    value = (value >>> 1) ^ (-(value & 1));
    return value;
  }

  /// Counts the number of bytes needed to encode the given int value in LEB128-64b9B ZigZag format
  /// @param value the value that would be encoded
  /// @return the number of bytes needed to LEB128-64b9B ZigZag encode the value
  static int sizeOf(int value) {
    int length = 0;
    value = (value << 1) ^ (value >> 31);
    if (value >>> 7 == 0) {
      length++;
    } else {
      length++;
      if (value >>> 14 == 0) {
        length++;
      } else {
        length++;
        if (value >>> 21 == 0) {
          length++;
        } else {
          length++;
          if (value >>> 28 == 0) {
            length++;
          } else {
            length++;
            length++;
          }
        }
      }
    }
    return length;
  }

  static int sizeOf(long value) {
    int length = 0;
    value = (value << 1) ^ (value >> 63);
    if (value >>> 7 == 0) {
      length++;
    } else {
      length++;
      if (value >>> 14 == 0) {
        length++;
      } else {
        length++;
        if (value >>> 21 == 0) {
          length++;
        } else {
          length++;
          if (value >>> 28 == 0) {
            length++;
          } else {
            length++;
            if (value >>> 35 == 0) {
              length++;
            } else {
              length++;
              if (value >>> 42 == 0) {
                length++;
              } else {
                length++;
                if (value >>> 49 == 0) {
                  length++;
                } else {
                  length++;
                  if (value >>> 56 == 0) {
                    length++;
                  } else {
                    length = length + 2;
                  }
                }
              }
            }
          }
        }
      }
    }
    return length;
  }
}
