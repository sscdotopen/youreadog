package de.tuberlin.dima.youreadog.hadoop.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Links implements Writable {

  private long[] links;

  public Links() {
  }

  public void set(long[] links) {
    this.links = links;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(links.length);
    for (long link : links) {
      out.writeLong(link);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    links = new long[in.readInt()];
    for (int n = 0; n < links.length; n++) {
      links[n] = in.readLong();
    }
  }
}
