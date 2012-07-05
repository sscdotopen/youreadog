package de.tuberlin.dima.youreadog;

import de.tuberlin.dima.youreadog.hadoop.ExtractionJob;
import org.apache.hadoop.util.ToolRunner;

public class Play {

  public static void main(String[] args) throws Exception {

    ToolRunner.run(new ExtractionJob(), new String[] {
        "--input", "/home/ssc/Entwicklung/datasets/clueweb-single/",
        "--output", "/tmp/clueweb"
    });

//    ToolRunner.run(new PageIndexJob(), new String[] {
//        "--input", "/tmp/clueweb",
//        "--output", "/tmp/pageIndex"
//    });
  }

}
