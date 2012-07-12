package de.tuberlin.dima.youreadog.hadoop.writables;

import com.google.common.collect.Iterables;
import de.tuberlin.dima.youreadog.extraction.Resource;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class PageData implements Writable {

  private String uri;
  private String observationTime;

  private String[] links;
  private String[] resources;

  public PageData() {
  }

  public void set(String uri, String observationTime, Set<String> allLinks, Iterable<Resource> allResources) {
    this.uri = uri;
    this.observationTime = observationTime;

    links = new String[allLinks.size()];
    int n = 0;
    for (String link : allLinks) {
      links[n++] = link;
    }

    n = 0;
    resources = new String[Iterables.size(allResources)];
    for (Resource resource : allResources) {
      resources[n++] = resource.url();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(uri);
    out.writeUTF(observationTime);
    out.writeInt(links.length);
    for (int n = 0; n < links.length; n++) {
      out.writeUTF(links[n]);
    }
    out.writeInt(resources.length);
    for (int n = 0; n < resources.length; n++) {
      out.writeUTF(resources[n]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    uri = in.readUTF();
    observationTime = in.readUTF();
    links = new String[in.readInt()];
    for (int n = 0; n < links.length; n++) {
      links[n] = in.readUTF();
    }
    resources = new String[in.readInt()];
    for (int n = 0; n < resources.length; n++) {
      resources[n] = in.readUTF();
    }
  }

  public String uri() {
    return uri;
  }

  public String[] links() {
    return links;
  }
}
