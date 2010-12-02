package org.apache.hadoop.io.simpleseekableformat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormat.OffsetPair;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Write data in Seekable File Format.
 * Data from a single write will be in a single data segment.
 *   
 * See {@link SimpleSeekableFormat}
 */
public class SimpleSeekableFormatOutputStream extends CompressionOutputStream implements Configurable {

  /**
   * This is a hint.  The actual max can go beyond this number if a lot of data are
   * sent via a single write.  A single write will always be in the same data segment.   
   */
  private static final int DEFAULT_MAX_UNCOMPRESSED_SEGMENT_LENGTH = 1024 * 1024;
  
  /**
   * dataSegmentOut is a wrapper stream that automatically inserts MetaDataBlocks
   * while writing out data segments.
   */
  final InterleavedOutputStream dataSegmentOut;
  /**
   * dataSegmentDataOut is a DataOutputStream wrapping dataSegmentOut.
   */
  private final DataOutputStream dataSegmentDataOut;
  
  private Configuration conf;
  private Class<? extends CompressionCodec> codecClass;
  private CompressionCodec codec;
  private int maxUncompressedSegmentLength;
  
  private OffsetPair lastOffsets = new OffsetPair();
  
  private final SimpleSeekableFormat.Buffer currentDataSegmentBuffer = new SimpleSeekableFormat.Buffer();

  public SimpleSeekableFormatOutputStream(OutputStream out) {
    this(new DataOutputStream(out));
  }
  
  /**
   * DataOutputStream allows easy write of integer, string etc.
   */
  protected SimpleSeekableFormatOutputStream(DataOutputStream out) {
    // We don't use the inherited field "out" at all.
    super(null);
    this.dataSegmentOut = 
        new InterleavedOutputStream(out,
            SimpleSeekableFormat.METADATA_BLOCK_LENGTH,
            SimpleSeekableFormat.DATA_BLOCK_LENGTH,
            new MetaDataProducer()
          );
    this.dataSegmentDataOut = new DataOutputStream(dataSegmentOut);
  }

  
  private static byte[] NULLS = new byte[1024]; 
  static {
    Arrays.fill(NULLS, (byte)0);
  }
  
  /**
   * This inner class provides the metadata block.
   * Note that it accesses the lastOffsets field.
   */
  class MetaDataProducer implements InterleavedOutputStream.MetaDataProducer {

    /**
     * @param out  The raw output stream.
     */
    @Override
    public void writeMetaData(DataOutputStream out, int metaDataBlockSize) throws IOException {
      // Magic header and version
      out.write(SimpleSeekableFormat.MAGIC_HEADER_BYTES);
      out.writeInt(SimpleSeekableFormat.VERSION);
      // Write out the offset pair
      out.writeLong(lastOffsets.uncompressedOffset);
      out.writeLong(lastOffsets.compressedOffset);
      
      // Fill up the bytes
      int left = metaDataBlockSize - SimpleSeekableFormat.MAGIC_HEADER_BYTES.length - 4 - 8 - 8;
      while (left > 0) {
        int toWrite = Math.min(left, NULLS.length);
        out.write(NULLS, 0, toWrite);
        left -= toWrite;
      }
    }
    
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // Set the codec
    codecClass = conf.getClass(SimpleSeekableFormat.FILEFORMAT_SSF_CODEC_CONF, null, 
        CompressionCodec.class);
    if (codecClass == null) {
      codec = null;
    } else {
      codec = ReflectionUtils.newInstance(codecClass, conf);
    }
    // Set the max segment length
    maxUncompressedSegmentLength = conf.getInt(
        SimpleSeekableFormat.FILEFORMAT_SSF_MAX_UNCOMPRESSED_SEGMENT_LENGTH,
        DEFAULT_MAX_UNCOMPRESSED_SEGMENT_LENGTH);
  }
  
  @Override
  public void write(int b) throws IOException {
    currentDataSegmentBuffer.write(b);
    flushIfNeeded();
  }
  
  /**
   * This function makes sure the whole buffer is written into the same data segment.
   */
  @Override
  public void write(byte[] b, int start, int length) throws IOException {
    currentDataSegmentBuffer.write(b, start, length);
    flushIfNeeded();
  }
  
  @Override
  public void close() throws IOException {
    flush();
    dataSegmentDataOut.close();
  }

  private void flushIfNeeded() throws IOException {
    if (currentDataSegmentBuffer.size() >= maxUncompressedSegmentLength) {
      flush();
    }
  }
  
  /**
   * Take the current data segment, optionally compress it,
   * calculate the crc32, and then write it out.
   */
  @Override
  public void flush() throws IOException {
    DataSegmentWriter currentDataSegment = new DataSegmentWriter(currentDataSegmentBuffer, codec);
    currentDataSegment.writeTo(dataSegmentDataOut);
    // Clear out the current buffer
    currentDataSegmentBuffer.reset();
    // Update the latest offsets
    lastOffsets.uncompressedOffset += currentDataSegmentBuffer.size();
    lastOffsets.compressedOffset = dataSegmentOut.getDataOffset();
  }

  @Override
  public void finish() throws IOException {
    throw new RuntimeException("SeekableFileOutputStream does not support finish()");
  }
  
  @Override
  public void resetState() throws IOException {
    throw new RuntimeException("SeekableFileOutputStream does not support resetState()");
  }
  
}
