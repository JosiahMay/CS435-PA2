package cs435.josiahm.pa2.drivers;

import cs435.josiahm.pa2.inputFormats.TFInputFormat;
import cs435.josiahm.pa2.mappers.JobThreeMapperEntry;
import cs435.josiahm.pa2.mappers.JobThreeMapperTerms;
import cs435.josiahm.pa2.reducers.JobThreeReducer;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Finds the top 3 sentence of give articles and summarizes them in order of appearance
 */
public class JobThreeDriverJoin {

  /**
   * Finds the top 3 sentence and summarizes them in order of appearance
   * @param args [0] articles to read dir, [1] output dir and were the IDF*TF values are
   * @throws IOException File not found or Write Error
   * @throws ClassNotFoundException Class not found
   * @throws InterruptedException Interrupted process
   */
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    // Set input paths
    Path inputIDFtf = new Path(args[1] + "/Job2/IDF.TF");
    Path inputData = new Path(args[0]);

    // Setup job
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Part 3, Sentence.TF.IDF");
    job.setJarByClass(JobThreeDriverJoin.class);
    job.setMapSpeculativeExecution(false);

    // Setup Mapper
    MultipleInputs.addInputPath(job, inputIDFtf, TFInputFormat.class, JobThreeMapperTerms.class);
    MultipleInputs.addInputPath(job, inputData, TextInputFormat.class, JobThreeMapperEntry.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(StringDoubleValue.class);

    // Setup Reducer
    job.setReducerClass(JobThreeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(10);

    // Setup path arguments
    Path output = new Path(args[1] + "/Job3/results");

    // Remove output path if it exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(output)) {
      hdfs.delete(output, true);
    }

    // Add files to distributed cache
    RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(
        inputIDFtf, true);

    while (files.hasNext()) {
      job.addCacheFile(files.next().getPath().toUri());
    }

    FileInputFormat.addInputPath(job, inputData);
    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
