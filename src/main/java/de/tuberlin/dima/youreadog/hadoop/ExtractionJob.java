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

import com.google.common.collect.Iterables;
import de.tuberlin.dima.youreadog.extraction.ResourceExtractor;
import de.tuberlin.dima.youreadog.hadoop.writables.ExtractedResources;
import de.tuberlin.dima.youreadog.hadoop.writables.Resource;
import edu.umd.cloud9.collection.clue.ClueWarcInputFormat;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

public class ExtractionJob extends HadoopJob {

  public static enum Counters {
    PAGES, RESOURCES
  }

  public static void main(String[] args) throws Exception {
    new ExtractionJob().run(args);
  }

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    JobConf conf = mapOnly(inputPath, outputPath, ClueWarcInputFormat.class, SequenceFileOutputFormat.class,
        CluewebExtractionMapper.class, Text.class, ExtractedResources.class);

    FileSystem.get(conf).delete(outputPath, true);
    JobClient.runJob(conf);

    return 0;
  }

  static class CluewebExtractionMapper extends MapReduceBase
      implements Mapper<Writable, ClueWarcRecord, Text, ExtractedResources> {

    private static final String URI_KEY = "WARC-Target-URI";
    private static final String DATE_KEY = "WARC-Date";

    //private final LinkExtractor linkExtractor = new LinkExtractor();
    private final ResourceExtractor resourceExtractor = new ResourceExtractor();

    private final Pattern newline = Pattern.compile("\n");
    private final Pattern headerSeparator = Pattern.compile(": ");

    private final ExtractedResources extractedResources = new ExtractedResources();

    @Override
    public void map(Writable key, ClueWarcRecord record, OutputCollector<Text, ExtractedResources> collector,
        Reporter reporter) throws IOException {

      String uri = record.getHeaderMetadataItem(URI_KEY);

      if (uri == null) {
        return;
      }

      System.out.println(uri);

      String observationTime = "";

      String[] headerItems = newline.split(record.getHeaderString());
      for (String headerItem : headerItems) {
        String[] tokens = headerSeparator.split(headerItem);
        if (DATE_KEY.equals(tokens[0]) && tokens.length > 1) {
          observationTime = tokens[1];
        }
      }

      //Set<String> links = linkExtractor.extractLinks(uri, record.getContentUTF8());
      Iterable<Resource> resources = resourceExtractor.extractResources(uri, record.getContentUTF8());

      reporter.incrCounter(ExtractionJob.Counters.PAGES, 1);
      //reporter.incrCounter(ExtractionJob.Counters.LINKS, links.size());
      reporter.incrCounter(ExtractionJob.Counters.RESOURCES, Iterables.size(resources));

      extractedResources.set(observationTime, resources);
      collector.collect(new Text(uri), extractedResources);
    }
  }

}
