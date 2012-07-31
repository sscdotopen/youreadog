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

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ExtractedResources implements Writable {

  private String observationTime;

  private Resource[] resources;

  public ExtractedResources() {
  }

  public void set( String observationTime, Iterable<Resource> allResources) {
    this.observationTime = observationTime;
    resources = Iterables.toArray(allResources, Resource.class);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(observationTime);
    out.writeInt(resources.length);
    for (int n = 0; n < resources.length; n++) {
      resources[n].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    observationTime = in.readUTF();
    resources = new Resource[in.readInt()];
    for (int n = 0; n < resources.length; n++) {
      resources[n] = new Resource();
      resources[n].readFields(in);
    }
  }

}
