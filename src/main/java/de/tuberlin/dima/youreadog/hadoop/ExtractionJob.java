package de.tuberlin.dima.youreadog.hadoop;

import com.google.common.collect.Iterables;
import de.tuberlin.dima.youreadog.extraction.LinkExtractor;
import de.tuberlin.dima.youreadog.extraction.ResourceExtractor;
import de.tuberlin.dima.youreadog.extraction.Resource;
import de.tuberlin.dima.youreadog.hadoop.writables.PageData;
import edu.umd.cloud9.collection.clue.ClueWarcInputFormat;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import java.util.Set;
import java.util.regex.Pattern;

public class ExtractionJob extends HadoopJob {

  public static enum Counters {
    PAGES, LINKS, RESOURCES
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
        CluewebExtractionMapper.class, NullWritable.class, PageData.class);

    FileSystem.get(conf).delete(outputPath, true);
    JobClient.runJob(conf);

    return 0;
  }

  static class CluewebExtractionMapper extends MapReduceBase
      implements Mapper<Writable, ClueWarcRecord, NullWritable, PageData> {

    private static final String URI_KEY = "WARC-Target-URI";
    private static final String DATE_KEY = "WARC-Date";

    private final LinkExtractor linkExtractor = new LinkExtractor();
    private final ResourceExtractor resourceExtractor = new ResourceExtractor();

    private final Pattern newline = Pattern.compile("\n");
    private final Pattern headerSeparator = Pattern.compile(": ");

    private final PageData pageData = new PageData();

    @Override
    public void map(Writable key, ClueWarcRecord record, OutputCollector<NullWritable, PageData> collector,
        Reporter reporter) throws IOException {

      String uri = record.getHeaderMetadataItem(URI_KEY);

      if (uri == null) {
        return;
      }

      //System.out.println(uri);

      String observationTime = "";

      String[] headerItems = newline.split(record.getHeaderString());
      for (String headerItem : headerItems) {
        String[] tokens = headerSeparator.split(headerItem);
        if (DATE_KEY.equals(tokens[0]) && tokens.length > 1) {
          observationTime = tokens[1];
        }
      }

      Set<String> links = linkExtractor.extractLinks(uri, record.getContentUTF8());
      Iterable<Resource> resources = resourceExtractor.extractResources(uri, record.getContentUTF8());

      reporter.incrCounter(ExtractionJob.Counters.PAGES, 1);
      reporter.incrCounter(ExtractionJob.Counters.LINKS, links.size());
      reporter.incrCounter(ExtractionJob.Counters.RESOURCES, Iterables.size(resources));

      pageData.set(uri, observationTime, links, resources);
      collector.collect(NullWritable.get(), pageData);
    }
  }

}
