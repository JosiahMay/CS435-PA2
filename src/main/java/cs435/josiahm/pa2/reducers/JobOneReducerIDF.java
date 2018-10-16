package cs435.josiahm.pa2.reducers;

import cs435.josiahm.pa2.drivers.JobOneDriver;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Calculates the IDF value for a term
 */
public class JobOneReducerIDF extends Reducer<Text, IntWritable, Text, DoubleWritable> {


  /**
   * Output for the IDF value
   */
  private final DoubleWritable outputValue = new DoubleWritable();
  /**
   * The total number of DOCID's in the document
   */
  private long totalIds;

  /**
   * Find the output directory and the Total number of document DOCID's in the files
   * @param context how to talk to HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    // Multiple output setup
    totalIds = context.getConfiguration().getLong(JobOneDriver.DocumentsCount.NUMDOCS.name(), 0);// get the N count
  }

  /**
   * Either finds the IDF value for a word or writes the TF for the DOCID and word
   * @param key The word for IDF or the TF info to print, choose by the reducer value in the key
   * @param values If IDF the DOCID's the word is in, If TF the TF value to print
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    //Find the number of DOCID that have the same word
    double count = getCount(values);

    outputValue.set(Math.log(totalIds/count)); // IDF value
    // Write to IDF directory
    context.write(key, outputValue);
    }


  /**
   * Gets the number of DOCID's the word was in
   * @param values the Id's of the word
   * @return the count
   */
  private double getCount(Iterable<IntWritable> values) {
    double count = 0;
    for (IntWritable item : values) {
      count += item.get();
    }
    return count;
  }


}
