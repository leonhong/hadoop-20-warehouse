package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * SimpleSeekableFormat supports seek based on compressed byte offsets as well
 * as uncompressed byte offsets.
 *
 * File Format Description:
 * 1. Metadata blocks and data blocks
 * Each 1K bytes at the beginning of x MB is a metadata block.
 * The rest of 1023K bytes are data blocks.
 * 
 * 2. Metadata block (1024 bytes):
 * Each metata block looks like this:
 * 32 bytes: "SSF_Magic_C17e5C697a00bB1A859aD\n"
 * 4 bytes: version number, now is 1.
 * 16 bytes: 8-byte of uncompressed data stream offset
 *           + 8-byte of compressed data stream offset
 * 
 * 3. Data block (1023 * 1024 bytes):
 * All data blocks should be concatenated to be a stream.  The stream consists
 * of consecutive data segments, back by back.
 * 
 * 4. Data segment:
 * Each data segment looks like this:
 * 4 bytes: length (implies that a single data segment cannot be longer than 
 *          4GB).  It does not include the length field itself, but includes 
 *          all following fields like codec name and crc32 checksum.
 * 2 bytes: byte length of compression codec class name.
 * x-bytes: UTF-8 encoded compression codec class name.
 * 8 bytes: crc32 checksum of the data following.
 * length - 8 - 2 - x bytes: actual data
 * 
 * This class encapsulates all underlying logics of the SeekableFileFormat.
 * 
 * NOTE: Requirement on the CompressionCodec InputStream: available() should
 * only return 0 when EOF.  Otherwise SeekableFileInputStream.available() will
 * break. 
 */
class SimpleSeekableFormat {

  public static final String FILEFORMAT_SSF_CODEC_CONF = "fileformat.ssf.codec";
  public static final String FILEFORMAT_SSF_MAX_UNCOMPRESSED_SEGMENT_LENGTH =
    "fileformat.ssf.max.uncompressed.segment.length";

  
  static final int METADATA_BLOCK_LENGTH = 1024;
  static final int DATA_BLOCK_LENGTH = 1024 * 1024 - METADATA_BLOCK_LENGTH;
  
  static final int VERSION = 1;
  static final String MAGIC_HEADER = "SSF_Magic_C17e5C697a00bB1A859aD\n";
  static final byte[] MAGIC_HEADER_BYTES;
  static {
    try {
      MAGIC_HEADER_BYTES = MAGIC_HEADER.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
  
  
  static class OffsetPair {
    long uncompressedOffset;
    long compressedOffset;
    
    void readFrom(DataInputStream in) throws IOException {
      uncompressedOffset = in.readLong();
      compressedOffset = in.readLong();
    }
    void writeTo(DataOutputStream out) throws IOException {
      out.writeLong(uncompressedOffset);
      out.writeLong(compressedOffset);
    }
  };
  
  static class Buffer extends ByteArrayOutputStream {
    public byte[] getData() { return buf; }
    public int getLength() { return count; }
    public void reset() { count = 0; }
  }
  
}
