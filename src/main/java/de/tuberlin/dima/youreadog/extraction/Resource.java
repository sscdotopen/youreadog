package de.tuberlin.dima.youreadog.extraction;


import com.google.common.base.Preconditions;

public class Resource {

  private final String url;
  private final Type type;

  public enum Type {
    LINK, IMAGE, SCRIPT, IFRAME, OTHER
  }

  public Resource(String url, Type type) {
    this.url = Preconditions.checkNotNull(url);
    this.type = type;
  }

  public String url() {
    return url;
  }

  public Type type() {
    return type;
  }

  @Override
  public int hashCode() {
    return 31 * type.hashCode() + url.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Resource) {
      Resource other = (Resource) obj;
      return type.equals(other.type) && url().equals(other.url);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Resource [" + url + ", " + type + "]";
  }

}