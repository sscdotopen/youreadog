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

import de.tuberlin.dima.youreadog.hadoop.writables.ExtractedResources;
import de.tuberlin.dima.youreadog.hadoop.writables.Resource;
import de.tuberlin.dima.youreadog.utils.Hashes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PageIndexJob extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    JobConf conf = mapOnly(inputPath, outputPath, SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
        HashMapper.class, LongWritable.class, Text.class);

    FileSystem.get(conf).delete(outputPath, true);
    JobClient.runJob(conf);

    return 0;

  }

  static class HashMapper extends MapReduceBase implements Mapper<Text, ExtractedResources, LongWritable, Text> {

    private final Text uri = new Text();
    private final LongWritable hash = new LongWritable();

    @Override
    public void map(Text pageUri, ExtractedResources extractedResources, OutputCollector<LongWritable, Text> out,
        Reporter reporter) throws IOException {
      hashAndCollect(pageUri.toString(), out);
      for (Resource resource : extractedResources.resources()) {
        if (Resource.Type.LINK.equals(resource.type())) {
          hashAndCollect(resource.url(), out);
        }
      }
    }

    private void hashAndCollect(String extractedUri, OutputCollector<LongWritable, Text> out) throws IOException {
      uri.set(extractedUri);
      hash.set(Hashes.hash64(extractedUri));
      out.collect(hash, uri);
    }
  }
}
