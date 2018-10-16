package cs435.josiahm.pa2.combiners;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobOneCombinerIDF extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  private final IntWritable outputValueIDF = new IntWritable();

  /**
   * Count the number of IDs the have a give term
   *
   * @param key The word
   * @param values the count of ID's that have the word
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    // Find the number of ID that have the same word
    int count = 0;
    for (IntWritable item : values) {
      count += item.get();
    }
    outputValueIDF.set(count);
    context.write(key, outputValueIDF);

  }
}
