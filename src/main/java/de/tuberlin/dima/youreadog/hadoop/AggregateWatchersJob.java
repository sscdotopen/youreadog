package de.tuberlin.dima.youreadog.hadoop;

import de.tuberlin.dima.youreadog.hadoop.io.ArcInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class AggregateWatchersJob extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    JobConf conf = mapReduce(inputPath, outputPath, SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
                             WatchersMapper.class, Text.class, LongWritable.class,
                             CountWatchingsReducer.class, Text.class, LongWritable.class);
    conf.setCombinerClass(CountWatchingsReducer.class);

    FileSystem.get(conf).delete(outputPath, true);
    JobClient.runJob(conf);

    return 0;
  }

  static class WatchersMapper extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {

    private final Text watcher = new Text();
    private final LongWritable one = new LongWritable(1);

    private static final Pattern SEP = Pattern.compile(",");

    @Override
    public void map(Text url, Text watchers, OutputCollector<Text, LongWritable> collector, Reporter reporter)
      throws IOException {

      String[] allWatchers = SEP.split(watchers.toString());
      for (String aWatcher : allWatchers) {
        watcher.set(aWatcher);
        collector.collect(watcher, one);
      }
    }
  }

  static class CountWatchingsReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable count = new LongWritable();

    @Override
    public void reduce(Text watcher, Iterator<LongWritable> counts, OutputCollector<Text, LongWritable> collector,
                       Reporter reporter) throws IOException {
      long sum = 0;
      while (counts.hasNext()) {
        sum += counts.next().get();
      }

      count.set(sum);
      collector.collect(watcher, count);
    }
  }
}
