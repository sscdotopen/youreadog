/**
 * Copyright (C) 2012 Database Systems and Information Management
 * Group of Technische Universität Berlin (http://www.dima.tu-berlin.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package de.tuberlin.dima.youreadog.hadoop.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongArrayWritable implements Writable {

  private long[] values;

  public LongArrayWritable() {
  }

  public void set(long[] values) {
    this.values = values;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);
    for (long value : values) {
      out.writeLong(value);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    values = new long[length];
    for (int n = 0; n < length; n++) {
      values[n] = in.readLong();
    }
  }
}
