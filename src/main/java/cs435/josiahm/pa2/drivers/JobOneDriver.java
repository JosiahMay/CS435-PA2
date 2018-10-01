package cs435.josiahm.pa2.drivers;

import cs435.josiahm.pa2.mappers.JobOneMapper;
import cs435.josiahm.pa2.reducers.JobOneReducer;
import cs435.josiahm.pa2.writableComparables.JobOneKey;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobOneDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    // Setup job
    Configuration conf = new Configuration();
    conf.set("OutPutDirectory", args[1] + "/Job1");
    Job job = Job.getInstance(conf, "Part 1, TF and IDF");

    // Setup Mapper
    job.setJarByClass(JobOneDriver.class);
    job.setMapperClass(JobOneMapper.class);

    job.setMapOutputKeyClass(JobOneKey.class);
    job.setMapOutputValueClass(StringDoubleValue.class);

    // Setup Reducer
    job.setReducerClass(JobOneReducer.class);
    job.setNumReduceTasks(10);

    // Setup multiple outputs
    MultipleOutputs
        .addNamedOutput(job, "TF", TextOutputFormat.class, Text.class, DoubleWritable.class);
    MultipleOutputs
        .addNamedOutput(job, "IDF", TextOutputFormat.class, Text.class, DoubleWritable.class);

    // Setup path arguments
    Path input = new Path(args[0]);
    Path output = new Path(args[1] + "/Job1");

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
