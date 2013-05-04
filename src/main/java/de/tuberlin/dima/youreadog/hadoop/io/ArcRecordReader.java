package de.tuberlin.dima.youreadog.hadoop.io;

import java.io.EOFException;
import java.io.IOException;

import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads ARC records.
 * 
 * Set "io.file.buffer.size" to define the amount of data that should be
 * buffered from S3.
 */
public class ArcRecordReader implements RecordReader<Text, ArcRecord> {

  private static final Logger log = LoggerFactory.getLogger(ArcRecordReader.class);

  private FSDataInputStream fsIn;
  private GzipCompressorInputStream gzipIn;
  private long fileLength;

  /**
   *
   */
  public ArcRecordReader(Configuration job, FileSplit split) throws IOException {

    if (split.getStart() != 0) {
      IOException ex = new IOException("Invalid ARC file split start " + split.getStart()
          + ": ARC files are not splittable");
      log.error(ex.getMessage(), ex);
      throw ex; 
    }

    // open the file and seek to the start of the split
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(job);

    fsIn = fs.open(file);

    // create a GZIP stream that *does not* automatically read through members
    gzipIn = new GzipCompressorInputStream(this.fsIn, false);

    fileLength = fs.getFileStatus(file).getLen();

    // First record should be an ARC file header record.  Skip it.
    skipRecord();
  }

  /**
   * Skips the current record, and advances to the next GZIP member.
   */
  private void skipRecord() throws IOException {

    long n = 0;

    do {
      n = gzipIn.skip(999999999);
    } while (n > 0);

    gzipIn.nextMember();
  }

  public Text createKey() {
    return new Text();
  }

  public ArcRecord createValue() {
    return new ArcRecord();
  }

  private static byte[] checkBuffer = new byte[64];

  public synchronized boolean next(Text key, ArcRecord value) throws IOException {

    boolean isValid = true;
    
    // try reading an ARC record from the stream
    try {
      isValid = value.readFrom(gzipIn);
    } catch (EOFException ex) {
      return false;
    }

    // if the record is not valid, skip it
    if (!isValid) {
      log.error("Invalid ARC record found at GZIP position " + this.gzipIn.getBytesRead() + ".  Skipping ...");
      skipRecord();
      return true;
    }

    if (value.getURL() != null) {
      key.set(value.getURL());
    }

    // check to make sure we've reached the end of the GZIP member
    int n = gzipIn.read(checkBuffer, 0, 64);

    if (n != -1) {
      log.error(n + "  bytes of unexpected content found at end of ARC record.  Skipping ...");
      skipRecord();
    }
    else {
      gzipIn.nextMember();
    }
   
    return true;
  }

  public float getProgress() throws IOException {
    return Math.min(1.0f, gzipIn.getBytesRead() / (float) fileLength);
  }

  public synchronized long getPos() throws IOException {
    return gzipIn.getBytesRead();
  }

  public synchronized void close() throws IOException {
    if (gzipIn != null) {
      gzipIn.close();
    }
  }

}
