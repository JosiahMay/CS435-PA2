package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobTwoMapperTF extends Mapper< Object, WordTermFrequency, Text, StringDoubleValue> {

  private final Text outputKeyTF = new Text();
  private final StringDoubleValue outputValueTF = new StringDoubleValue();

  public void map(Object key, WordTermFrequency value, Context context)
      throws IOException, InterruptedException {

    outputKeyTF.set(value.word);
    outputValueTF.set(value.id, value.tfValue);

    context.write(outputKeyTF, outputValueTF);


  }

}
