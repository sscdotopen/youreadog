package de.tuberlin.dima.youreadog.hadoop;

import com.google.common.collect.Maps;
import de.tuberlin.dima.youreadog.hadoop.writables.PageData;
import edu.umd.cloud9.collection.clue.ClueWarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * a very simple base class for hadoop jobs
 */
public abstract class HadoopJob extends Configured implements Tool {

  protected Map<String,String> parseArgs(String[] args) {
    if (args == null || args.length % 2 != 0) {
      throw new IllegalStateException("Cannot convert args!");
    }

    Map<String,String> parsedArgs = Maps.newHashMap();
    for (int n = 0; n < args.length; n+=2) {
      parsedArgs.put(args[n], args[n+1]);
    }
    return Collections.unmodifiableMap(parsedArgs);
  }

  protected JobConf mapOnly(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
      Class keyClass, Class valueClass) {
    JobConf conf = new JobConf(getClass());
    conf.setJobName(getClass().getSimpleName() + "-" + mapperClass.getSimpleName());

    conf.setNumReduceTasks(0);

    FileOutputFormat.setOutputPath(conf, output);
    FileOutputFormat.setCompressOutput(conf, false);

    FileInputFormat.addInputPath(conf, input);
    conf.setInputFormat(inputFormatClass);

    conf.setOutputFormat(outputFormatClass);
    conf.setOutputKeyClass(keyClass);
    conf.setOutputValueClass(valueClass);

    conf.setMapperClass(mapperClass);

    return conf;
  }

}