package org.apache.hadoop.io.simpleseekableformat;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.compress.CompressionInputStream;

/**
 * The reader for Seekable File Format.
 *
 * See {@link SimpleSeekableFormat}
 */
public class SimpleSeekableFormatInputStream extends CompressionInputStream {

  private final DataInputStream dataIn;
  private InputStream dataSegmentIn;
  
  public SimpleSeekableFormatInputStream(InputStream in) {
    // we don't use the inherited field "in" at all:
    super(null);
    this.dataIn = new DataInputStream(new InterleavedInputStream(in,
        SimpleSeekableFormat.METADATA_BLOCK_LENGTH,
        SimpleSeekableFormat.DATA_BLOCK_LENGTH));
  }

  @Override
  public int read() throws IOException {
    if (dataSegmentIn == null) {
      if (!moveToNextDataSegment()) {
        return -1;
      }
    }
    do {
      int result = dataSegmentIn.read();
      if (result != -1) {
        return result;
      }
      if (!moveToNextDataSegment()) {
        return -1;
      }
    } while (true);
  }

  @Override
  public int read(byte[] b, int start, int length) throws IOException {
    if (dataSegmentIn == null) {
      if (!moveToNextDataSegment()) {
        return -1;
      }
    }
    do {
      int result = dataSegmentIn.read(b, start, length);
      if (result != -1) {
        return result;
      }
      if (!moveToNextDataSegment()) {
        return -1;
      }
    } while (true);
  }
  
  @Override
  public void close() throws IOException {
    dataIn.close();
  }
  
  /**
   * This function depends on that the underlying dataSegmentIn.available() only
   * returns 0 when EOF.  Otherwise it will break because it jumps over the dataSegmentIn
   * that has available() == 0.
   */
  @Override
  public int available() throws IOException {
    if (dataSegmentIn == null) {
      if (!moveToNextDataSegment()) {
        return 0;
      }
    }
    do {
      int result = dataSegmentIn.available();
      if (result != 0) {
        return result;
      }
      if (!moveToNextDataSegment()) {
        return -1;
      }
    } while (true);
  }
  
  /**
   * Returns false if there are no more data segments.
   */
  private boolean moveToNextDataSegment() throws IOException {
    if (dataIn.available() == 0) {
      return false;
    }
    try {
      DataSegmentReader dataSegmentReader = new DataSegmentReader(dataIn);
      dataSegmentIn = dataSegmentReader.getInputStream();
    } catch (EOFException e) {
      // EOFException is thrown when the underlying data stream is truncated, e.g. truncated file.
      // This is considered as a normal case.
      return false;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public void resetState() throws IOException {
    throw new RuntimeException("SeekableFileInputFormat does not support resetState()");
  }
  
}
