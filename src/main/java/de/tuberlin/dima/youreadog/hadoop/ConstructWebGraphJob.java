package de.tuberlin.dima.youreadog.hadoop;


import de.tuberlin.dima.youreadog.HashUtils;
import de.tuberlin.dima.youreadog.hadoop.writables.Links;
import de.tuberlin.dima.youreadog.hadoop.writables.PageData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class ConstructWebGraphJob extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    JobConf conf = mapOnly(inputPath, outputPath, SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
        ToLinksMapper.class, NullWritable.class, Text.class);

    FileSystem.get(conf).delete(outputPath, true);
    JobClient.runJob(conf);

    return 0;
  }

  static class ToLinksMapper extends MapReduceBase
      implements Mapper<NullWritable, PageData, LongWritable, Links> {

    private final LongWritable page = new LongWritable();
    private final Links links = new Links();

    @Override
    public void map(NullWritable key, PageData pageData, OutputCollector<LongWritable, Links> collector,
        Reporter reporter) throws IOException {

      long pageID = HashUtils.hash(pageData.uri());

      long[] linkIDs = new long[pageData.links().length];
      int n = 0;
      for (String link : pageData.links()) {
        linkIDs[n++] = HashUtils.hash(link);
      }

      page.set(pageID);
      links.set(linkIDs);

      collector.collect(page, links);
    }

  }




}
