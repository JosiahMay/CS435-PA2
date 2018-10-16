package cs435.josiahm.pa2.drivers;

import cs435.josiahm.pa2.combiners.JobOneCombinerIDF;
import cs435.josiahm.pa2.mappers.JobOneMapperIDF;
import cs435.josiahm.pa2.mappers.JobOneMapperTF;
import cs435.josiahm.pa2.reducers.JobOneReducerIDF;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobOneDriver {

  /**
   * Counts the number of document IDs with articles
   */
  public enum DocumentsCount {
    NUMDOCS
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    // Setup job
    Configuration conf = new Configuration();
    Path input = new Path(args[0]);

    // Find TF values
    Job firstJob = Job.getInstance(conf, "Part 1, TF");

    // Setup Mapper
    firstJob.setJarByClass(JobOneDriver.class);
    firstJob.setMapperClass(JobOneMapperTF.class);
    firstJob.setMapOutputKeyClass(Text.class);
    firstJob.setMapOutputValueClass(Text.class);


    // NO Reducers
    firstJob.setNumReduceTasks(0);


    // Setup path arguments
    Path outputTF = new Path(args[1] + "/Job1/TF");

    FileInputFormat.addInputPath(firstJob, input);

    // Remove output path if it exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputTF)) {
      hdfs.delete(outputTF, true);
    }

    FileOutputFormat.setOutputPath(firstJob, outputTF);

    // Wait for TF job to finish
    if(!firstJob.waitForCompletion(true)){
      System.exit(1);
    }

    // Get the count if DOCID from the TF job
    Counter documentCount = firstJob.getCounters().findCounter(DocumentsCount.NUMDOCS);

    // Set up IDF job
    Job secondJob = Job.getInstance(conf, "Part 1, IDF");

    // Save ID count to IDF job
    secondJob.getConfiguration().setLong(documentCount.getDisplayName(), documentCount.getValue());

    // Setup Mapper
    secondJob.setJarByClass(JobOneDriver.class);
    secondJob.setMapperClass(JobOneMapperIDF.class);
    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(IntWritable.class);

    // Set combiner
    secondJob.setCombinerClass(JobOneCombinerIDF.class);


    // Setup Reducer
    secondJob.setReducerClass(JobOneReducerIDF.class);
    secondJob.setOutputKeyClass(Text.class);
    secondJob.setOutputValueClass(DoubleWritable.class);


    // Setup path arguments
    Path output = new Path(args[1] + "/Job1/IDF");

    FileInputFormat.addInputPath(secondJob, input);

    // Remove output path if it exists
    if (hdfs.exists(output)) {
      hdfs.delete(output, true);
    }

    FileOutputFormat.setOutputPath(secondJob, output);

    System.exit(secondJob.waitForCompletion(true) ? 0 : 1);

  }
}
