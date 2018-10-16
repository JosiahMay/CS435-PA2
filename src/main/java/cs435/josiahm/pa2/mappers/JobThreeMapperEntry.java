package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads the article and send each sentence to the reduce with the ID as a key
 */
public class JobThreeMapperEntry extends
    Mapper< Object, Text, Text, StringDoubleValue> {

  /**
   * The article ID
   */
  private final Text outputKey = new Text();
  /**
   * The sentence and its order in the article
   */
  private final StringDoubleValue outputValue = new StringDoubleValue();

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] parts = value.toString().split("<====>");

    // See if article is valid
    if(parts.length != 3){
      return;
    }


    String id = parts[1];
    // Find all the sentences
    String[] sentences = parts[2].split("\\.\\s");

    outputKey.set(id);

    //Send each sentence with its order
    for(int i = 0; i < sentences.length; i++){
      outputValue.set("Entry<====>" + sentences[i], (double) i);
      context.write(outputKey, outputValue);
    }
  }



}
