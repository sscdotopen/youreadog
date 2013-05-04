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

package de.tuberlin.dima.youreadog.hadoop;

import de.tuberlin.dima.youreadog.hadoop.util.TopWatchers;
import org.apache.hadoop.util.ToolRunner;

public class CommonCrawlSampleIntegrationTest {

  public static void main(String[] args) throws Exception {

    ExtractionJob extraction = new ExtractionJob();
    AggregateWatchersJob aggregateWatchers = new AggregateWatchersJob();

    ToolRunner.run(extraction, new String[] {
        "--input", "/home/ssc/Entwicklung/projects/youreadog/src/test/resources/commoncrawl/",
        "--output", "/tmp/commoncrawl-extraction/"
    });

    ToolRunner.run(aggregateWatchers, new String[] {
        "--input", "/tmp/commoncrawl-extraction/",
        "--output", "/tmp/commoncrawl-watchers/"
    });

    TopWatchers.print("/tmp/commoncrawl-watchers/part-00000", 100);
  }
}