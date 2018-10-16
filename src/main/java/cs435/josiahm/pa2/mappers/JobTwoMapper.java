package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads All the TF values and calculates the IDF*TF value
 */
public class JobTwoMapper extends Mapper< Object, WordTermFrequency, WordTermFrequency, NullWritable> {

  /**
   * Null for output value
   */
  private NullWritable nullOutput;
  /**
   * All the IDF values from cache
   */
  private HashMap<String, Double> idfValues = new HashMap<>();

  @Override
  /**
   * Reads the IDF values from cache
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  protected void setup(Context context) throws IOException, InterruptedException {

    // Get files from the cache
    FileSystem hdfs = FileSystem.get(context.getConfiguration());
    URI[] idfFiles = context.getCacheFiles();

    for (URI file : idfFiles) {
      Path path = new Path(file.getPath());

      if (hdfs.exists(path)) {

        BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(path)));
        processWord(bfr);
        bfr.close();

      } else {
        throw new FileNotFoundException(file.getPath());
      }

    }

  }

  /**
   * Process each IDF value
   * @param bfr  the buffer reader to use
   * @throws IOException if the read fails
   */
  private void processWord(BufferedReader bfr) throws IOException {

    String line = null;
    while ((line = bfr.readLine()) != null) {
      String[] parts = line.split("\t");
      String word = parts[0];
      Double value = Double.parseDouble(parts[1]);
      idfValues.put(word, value);
    }

  }


  /**
   * Reads All the TF values and calculates the IDF*TF value
   * @param key unused
   * @param value the TF value
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  public void map(Object key, WordTermFrequency value, Context context)
      throws IOException, InterruptedException {

    if(idfValues.containsKey(value.word)){
      value.tfValue = value.tfValue * idfValues.get(value.word);
      context.write(value, nullOutput);
    } else {
      throw new RuntimeException("missing wordsToTokenize" + value.toString());
    }
  }

}
