package cs435.josiahm.pa2.drivers;

import cs435.josiahm.pa2.mappers.JobThreeMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobThreeDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    // Set input paths
    Path inputIDFtf = new Path(args[1] + "/Job2/IDF.TF");
    Path inputData = new Path(args[0]);

    // Setup job
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Part 3, Sentence.TF.IDF");
    job.setJarByClass(JobThreeDriver.class);

    // Setup Mapper
    job.setJarByClass(JobThreeDriver.class);
    job.setMapperClass(JobThreeMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // Setup Reducer
    job.setNumReduceTasks(0);

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
