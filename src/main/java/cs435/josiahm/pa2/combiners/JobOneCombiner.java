package cs435.josiahm.pa2.combiners;

import cs435.josiahm.pa2.writableComparables.JobOneKey;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

public class JobOneCombiner extends
    Reducer<JobOneKey, StringDoubleValue, JobOneKey, StringDoubleValue> {

  /**
   * Identifier for the TF question and to create output path
   */
  private final String TF = "TF";

  /**
   * Identifier for the IDF question and to create output path
   */
  private final String IDF = "IDF";

  private final StringDoubleValue outputValueIDF = new StringDoubleValue();


  /**
   * Either finds the IDF value for a word or writes the TF for the ID and word
   * @param key The word for IDF or the TF info to print, choose by the reducer value in the key
   * @param values If IDF the ID's the word is in, If TF the TF value to print
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  @Override
  protected void reduce(JobOneKey key, Iterable<StringDoubleValue> values, Context context)
      throws IOException, InterruptedException {

    // Check if we just need to write the TF values
    switch (key.reducer) {
      case TF:
        // Go through all the words for the ID
        for (StringDoubleValue item : values) {
          context.write(key, item);
        }
        break;
      case IDF:
        // Find the number of ID that have the same word
        double count = 0;
        for (StringDoubleValue item : values) {
          count += item.value;
        }
        outputValueIDF.set("IDF VALUE", count);
        context.write(key, outputValueIDF);

        break;
      default:
        // Fail when a key is not sent to the correct reducer
        throw new RuntimeException(key.reducer);
    }
  }

}
