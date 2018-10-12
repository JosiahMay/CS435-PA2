package cs435.josiahm.pa2.reducers;

import cs435.josiahm.pa2.util.IdCounter.TOTALS;
import cs435.josiahm.pa2.writableComparables.JobOneKey;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

public class JobOneReducer extends Reducer<JobOneKey, StringDoubleValue, Text, DoubleWritable> {

  /**
   * Identifier for the TF question and to create output path
   */
  private final String TF = "TF";

  /**
   * Identifier for the IDF question and to create output path
   */
  private final String IDF = "IDF";

  /**
   * Output for the key Word or ID\tWord
   */
  private final Text outputKey = new Text();

  /**
   * Output for TF or IDF value
   */
  private final DoubleWritable outputValue = new DoubleWritable();

  /**
   * Used to write to multiple location
   */
  private MultipleOutputs mos;

  /**
   * The total number of ID's in the document
   */
  private long totalIds;

  /**
   * The base output directory for TF files
   */
  private String outputDirTF;

  /**
   * The base output directory for IDF files
   */
  private String outputDirIDF;

  /**
   * Find the output directory and the Total number of document ID's in the files
   * @param context how to talk to HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    // Multiple output setup
    mos = new MultipleOutputs(context);

    // Get number of IDs
    Configuration conf = context.getConfiguration();  // get the config for the job
    Cluster cluster = new Cluster(conf); // find were the jobs are running on yarn
    Job currentJob = cluster.getJob(context.getJobID()); // find what the current job is
    totalIds = currentJob.getCounters().findCounter(TOTALS.ID).getValue(); // get the N count

    // Get the output dir
    String outputDir = conf.get("OutPutDirectory") + "/";
    outputDirTF = outputDir + TF + "/" + TF;
    outputDirIDF = outputDir + IDF + "/" + IDF;

  }

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
          if(item.id.equals("IDF VALUE"))
          {
            mos.write("MissLabeled", new Text(key.key + "\t" + key.reducer + "\t" + item.id), new DoubleWritable(item.value));
            return;
          }
          writeTF(key, item);

        }
        break;
      case IDF:
        // Find the number of ID that have the same word
        double count = getCount(values);

        writeIDF(key, count);

        break;
      default:
        // Fail when a key is not sent to the correct reducer
        throw new RuntimeException(key.reducer);
    }
  }

  /**
   * Gets the number of ID's the word was in
   * @param values the Id's of the word
   * @return the count
   */
  private double getCount(Iterable<StringDoubleValue> values) {
    double count = 0;
    for (StringDoubleValue item : values) {
      count += item.value;
    }
    return count;
  }

  /**
   * Writes the IDF value for a word
   * @param key the word
   * @param count the number of ID's the word was used in
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  private void writeIDF(JobOneKey key, double count) throws IOException, InterruptedException {
    // Set outputs
    outputKey.set(key.key);
    outputValue.set(Math.log(totalIds/count)); // IDF value
    // Write to IDF directory
    mos.write(IDF, outputKey, outputValue, outputDirIDF);
  }

  /**
   * Writes the TF value for the ID word Pair
   * @param key The ID of the words
   * @param item the Words and TF value for the words
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  private void writeTF(JobOneKey key, StringDoubleValue item)
      throws IOException, InterruptedException {
    // Set outputs
    outputKey.set(key.key + "\t" + item.id);
    outputValue.set(item.value);
    // Write to TF directory
    mos.write(TF, outputKey, outputValue, outputDirTF);
  }

}
