package org.apache.hadoop.io.simpleseekableformat;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * This InputStream removes the metadata from the underlying stream. 
 */
class InterleavedInputStream extends InputStream {

  public interface MetaDataConsumer {
    /**
     * This function should read a metadata block with size metaDataBlockSize.
     * This function should throw EOFException if there are not enough bytes
     * in the InputStream.
     * @param in  The raw input stream
     */
    void readMetaData(InputStream in, int metaDataBlockSize) throws IOException;
  }
  
  public static class DefaultMetaDataConsumer implements MetaDataConsumer {
    @Override
    public void readMetaData(InputStream in, int metaDataBlockSize)
        throws IOException {
      long toSkip = metaDataBlockSize;
      while (toSkip > 0) {
        long skipped = in.skip(toSkip);
        if (skipped <= 0) {
          throw new EOFException("Incomplete metadata section.  Should be "
              + metaDataBlockSize + " bytes but got only "
              + (metaDataBlockSize - toSkip)
              + " bytes and then skip returns " + skipped);
        }
        toSkip -= skipped;
      }
    }
  }
  
  private final InputStream in;
  private final int metaDataBlockSize;
  private final int dataBlockSize;
  private final MetaDataConsumer metaDataConsumer;
  
  private long completeMetaDataBlocks;
  private int currentDataBlockSize;
  private boolean eofReached;
  
  /**
   * @param in  THe code assumes that (in.available() == 0) means EOF.
   *            The code will break if that requirement is not met by
   *            the InputStream in.  Note that most InputStreams like
   *            {@link java.util.zip.InflaterInputStream#available()}
   *            support this assumption. 
   */
  InterleavedInputStream(InputStream in,
      int metaDataBlockSize, int dataBlockSize,
      MetaDataConsumer metaDataConsumer) {
    this.in = in;
    this.metaDataBlockSize = metaDataBlockSize;
    this.dataBlockSize = dataBlockSize;
    this.metaDataConsumer = metaDataConsumer;
    // Signal that we need to read metadata block first.
    currentDataBlockSize = dataBlockSize;
    eofReached = false;
  }
  
  /**
   * @param in  in.available() should return > 0 unless EOF
   */
  InterleavedInputStream(InputStream in,
      int metaDataBlockSize, int dataBlockSize) {
    this(in, metaDataBlockSize, dataBlockSize, new DefaultMetaDataConsumer());
  }
  
  /**
   * Number of bytes read from the underlying stream.
   */
  public long getOffset() {
    return completeMetaDataBlocks * metaDataBlockSize
      + getDataOffset();
  }
  
  /**
   * Number of data bytes read from the underlying stream.
   */
  public long getDataOffset() {
    return (completeMetaDataBlocks - 1) * dataBlockSize
        + currentDataBlockSize;
  }

  /**
   * Returns whether we've reached EOF.
   */
  private boolean readMetaDataIfNeeded() throws IOException {
    if (eofReached) {
      return false;
    }
    if (currentDataBlockSize == dataBlockSize) {
      try {
        metaDataConsumer.readMetaData(in, metaDataBlockSize);
        completeMetaDataBlocks ++;
        currentDataBlockSize = 0;
      } catch (EOFException e) {
        eofReached = true;
        return false;
      }
    }
    return true;
  }  
  
  @Override
  public int read() throws IOException {
    if (!readMetaDataIfNeeded()) {
      return -1;
    }
    int result = in.read();
    if (result >= 0) {
      // don't do this if read() returns -1, which means EOF.
      currentDataBlockSize ++;
    } else {
      eofReached = true;
    }
    return result;
  }

  @Override
  public int read(byte[] b, int start, int length) throws IOException {
    if (!readMetaDataIfNeeded()) {
      return -1;
    }
    int toRead = Math.min(length, dataBlockSize - currentDataBlockSize);
    int read = in.read(b, start, toRead);
    if (read >= 0) {
      currentDataBlockSize += read;
    } else {
      eofReached = true;
    }
    return read;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
  
  @Override
  public int available() throws IOException {
    int rawAvailable = in.available();
    // Before the next meta block
    int currentBlockLeft = Math.min(dataBlockSize - currentDataBlockSize, rawAvailable);
    rawAvailable -= currentBlockLeft;
    // How many full blocks are there?
    int fullBlocks = rawAvailable / (metaDataBlockSize + dataBlockSize);
    // How many bytes left besides the full blocks?
    rawAvailable = rawAvailable % (metaDataBlockSize + dataBlockSize);
    // Anything partial data blocks?
    int partialBlockLeft = Math.max(0, rawAvailable - metaDataBlockSize);
    
    return currentBlockLeft + fullBlocks * dataBlockSize + partialBlockLeft;    
  }
}
