package org.apache.hadoop.io.simpleseekableformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormat;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormatInputStream;
import org.apache.hadoop.io.simpleseekableformat.SimpleSeekableFormatOutputStream;


/**
 * TestCase for {@link SimpleSeekableFormatInputStream} and {@link SimpleSeekableFormatOutputStream}
 */
public class TestSimpleSeekableFormatStreams extends TestCase {

  public void testNormalWriteAndRead() throws Exception {
    testNormalWriteAndRead(null);
    testNormalWriteAndRead(GzipCodec.class);
  }
  
  void testNormalWriteAndRead(final Class<? extends CompressionCodec> codecClass
      ) throws Exception {
    // Not using loops here so we can know the exact parameter values from
    // the stack trace.
    testNormalWriteAndRead(codecClass, 1, 65536);
    testNormalWriteAndRead(codecClass, 100, 16384);
    testNormalWriteAndRead(codecClass, 1000, 4096);
  }
  
  /**
   * @param writeSize  0: use "write(int)"; > 0: use "write(byte[])".
   * @param readSize   0: use "read()"; > 0: use "read(bytes[])".
   */
  void testNormalWriteAndRead(final Class<? extends CompressionCodec> codecClass,
      final int numRecord, final int maxRecordSize) throws Exception {

    long startMs = System.currentTimeMillis();
    
    // Random seed for data to be written
    final Random dataRandom = new Random(333);
    // Random seed for verifying data written
    final Random dataRandom2 = new Random(333);

    // Create the in-memory file and start to write to it.
    ByteArrayOutputStream inMemoryFile = new ByteArrayOutputStream(); 
    SimpleSeekableFormatOutputStream out = new SimpleSeekableFormatOutputStream(inMemoryFile);
    
    // Set compression Codec
    Configuration conf = new Configuration();
    if (codecClass != null) {
      conf.setClass(SimpleSeekableFormat.FILEFORMAT_SSF_CODEC_CONF, codecClass, 
          CompressionCodec.class);
    }
    out.setConf(conf);
    
    // Write some data
    for (int r = 0; r < numRecord; r++) {
      byte[] b = new byte[dataRandom.nextInt(maxRecordSize)];
      // Generate some compressible random data
      TestUtils.nextBytes(dataRandom, b, 16);
      out.write(b);
    }
    out.close();
    
    long writeDoneMs = System.currentTimeMillis();
    
    // Open the in-memory file for read
    ByteArrayInputStream fileForRead = new ByteArrayInputStream(inMemoryFile.toByteArray());
    SimpleSeekableFormatInputStream in = new SimpleSeekableFormatInputStream(fileForRead);
    DataInputStream dataIn = new DataInputStream(in);
    
    // Verify the data
    for (int r = 0; r < numRecord; r++) {
      // Regenerate the same random bytes
      byte[] b = new byte[dataRandom2.nextInt(maxRecordSize)];
      TestUtils.nextBytes(dataRandom2, b, 16);
      // Read from the file
      byte[] b2 = new byte[b.length];
      dataIn.readFully(b2);
      TestUtils.assertArrayEquals("record " + r + " with length " + b.length,
          b, b2);
    }
    
    // Verify EOF
    Assert.assertEquals(-1, in.read());
    byte[] temp = new byte[100];
    Assert.assertEquals(-1, in.read(temp));
    
    long readDoneMs = System.currentTimeMillis();
    
    // Output file size and time used for debugging purpose
    System.out.println("File size = " + inMemoryFile.size()
        + " writeMs=" + (writeDoneMs - startMs) 
        + " readMs=" + (readDoneMs - writeDoneMs)
        + " numRecord=" + numRecord + " maxRecordSize=" + maxRecordSize
        + " codec=" + codecClass);

  }
  
}
