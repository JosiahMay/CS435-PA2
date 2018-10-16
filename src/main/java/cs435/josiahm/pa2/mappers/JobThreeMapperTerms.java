package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads all the IDF*TF values and sends them to the reducer
 */
public class JobThreeMapperTerms  extends Mapper< Object, WordTermFrequency, Text, StringDoubleValue> {

  /**
   * The article ID
   */
  private final Text outputKey = new Text();
  /**
   * The IDF*TF values
   */
  private final StringDoubleValue outputValue = new StringDoubleValue();

  /**
   * Reads all the IDF*TF values and sends them to the reducer
   * @param key unused
   * @param value The IDF*TF value
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  public void map(Object key, WordTermFrequency value, Context context)
      throws IOException, InterruptedException {

    outputKey.set(value.id);
    outputValue.set(value.word, value.tfValue);
    context.write(outputKey, outputValue);
  }

}
