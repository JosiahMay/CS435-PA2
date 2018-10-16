package cs435.josiahm.pa2.drivers;

import cs435.josiahm.pa2.inputFormats.TFInputFormat;
import cs435.josiahm.pa2.mappers.JobTwoMapper;
import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Finds the IDF*IF value for the corpus
 */
public class JobTwoDriver {

  /**
   * Finds the IDF*IF value for the corpus
   * @param args [0] input dir, [1] output dir
   * @throws IOException File not found or Write Error
   * @throws ClassNotFoundException Class not found
   * @throws InterruptedException Interrupted process
   */
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    Path inputTF = new Path(args[0] + "/Job1/TF");
    Path inputIDF = new Path(args[0] + "/Job1/IDF");

    // Setup job
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Part 2, TF*IDF");
    job.setJarByClass(JobTwoDriver.class);


    // Setup Mapper
    job.setMapperClass(JobTwoMapper.class);
    job.setInputFormatClass(TFInputFormat.class);
    job.setMapOutputKeyClass(WordTermFrequency.class);
    job.setMapOutputValueClass(NullWritable.class);

    // No Reducer
    job.setNumReduceTasks(0);


    // Setup path arguments
    Path output = new Path(args[1] + "/Job2/IDF.TF");

    // Remove output path if it exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(output)) {
      hdfs.delete(output, true);
    }

    // Add files to distributed cache
    RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(
        inputIDF, true);

    while(files.hasNext()){
      job.addCacheFile(files.next().getPath().toUri());
    }



    FileInputFormat.addInputPath(job, inputTF);
    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
