package cs435.josiahm.pa2.drivers;

import cs435.josiahm.pa2.mappers.JobTwoMapperTF;
import cs435.josiahm.pa2.reducers.JobTwoReducer;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobTwoDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    // Setup job
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Part 2, TF*IDF");

    // Setup Mapper
    job.setJarByClass(JobTwoDriver.class);
    job.setMapperClass(JobTwoMapperTF.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(StringDoubleValue.class);

    // Setup Reducer
    job.setReducerClass(JobTwoReducer.class);
    job.setNumReduceTasks(10);

    // Setup multiple outputs
    /*
    MultipleOutputs
        .addNamedOutput(job, "TF", TextOutputFormat.class, Text.class, DoubleWritable.class);
    MultipleOutputs
        .addNamedOutput(job, "IDF", TextOutputFormat.class, Text.class, DoubleWritable.class);
    */

    // Setup path arguments
    Path input = new Path(args[0]);
    Path output = new Path(args[1] + "/Job2/IDf*TF");

    FileInputFormat.addInputPath(job, input);

    // Remove output path if it exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(output)) {
      hdfs.delete(output, true);
    }
    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
