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

package de.tuberlin.dima.youreadog.utils;

import com.google.common.base.Charsets;
import org.apache.mahout.math.MurmurHash;

public class Hashes {

  private static final int SEED = 0xbeef;

  private Hashes() {}

  public static long hash64(String value) {
    return MurmurHash.hash64A(value.getBytes(Charsets.UTF_8), SEED);
  }
}
