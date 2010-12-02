package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class holds the data related to a single data segment.
 */
class DataSegmentReader {

  // uncompressed data stream
  private final InputStream uncompressedData;

  /**
   * Create a new data segment by reading from an input stream.
   * @throws EOFException if the underlying data stream is truncated (incomplete DataSegment) 
   */
  DataSegmentReader(DataInputStream in)
      throws IOException, ClassNotFoundException {
    this(in, new Configuration());
  }
  
  @SuppressWarnings("unchecked")
  DataSegmentReader(DataInputStream in, Configuration conf)
      throws IOException, ClassNotFoundException {
    
    // Read from DataInputStream
    // 1. Read length
    int length = in.readInt();
    // 2. Read codec
    int codecNameUTF8Length = in.readShort();
    byte[] codecNameUTF8 = new byte[codecNameUTF8Length];
    in.readFully(codecNameUTF8);
    String codecName = new String(codecNameUTF8, "UTF-8");
    // 3. read CRC32
    long crc32Value = in.readLong();
    // 4. read data
    byte[] storedData = new byte[length - 8/*crc32*/ - 2/*codec length*/ - codecNameUTF8Length];
    in.readFully(storedData);
    
    // Verify the checksum
    CRC32 crc32 = new CRC32();
    crc32.update(storedData);
    if (crc32.getValue() != crc32Value) {
      throw new CorruptedDataException("Corrupted data segment with length " + length
          + " crc32 expected " + crc32Value + " but got " + crc32.getValue());
    }
    
    // Uncompress the data if needed
    if (codecName.equals("")) {
      // no compression
      uncompressedData = new ByteArrayInputStream(storedData);
    } else {
      Class<? extends CompressionCodec> codecClass =
        (Class<? extends CompressionCodec>)Class.forName(codecName);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      uncompressedData = codec.createInputStream(new ByteArrayInputStream(storedData));     
    }
  }
  
  InputStream getInputStream() {
    return uncompressedData;
  }
  
}
