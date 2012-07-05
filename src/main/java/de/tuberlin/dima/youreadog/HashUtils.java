package de.tuberlin.dima.youreadog;

import com.google.common.base.Charsets;
import org.apache.mahout.math.MurmurHash;

public class HashUtils {

  private static final int SEED = 746552343;

  private HashUtils() {
  }

  public static long hash(String data) {
    return MurmurHash.hash64A(data.getBytes(Charsets.UTF_8), SEED);
  }

}
