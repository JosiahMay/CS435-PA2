package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.InvertedDocumentFrequency;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobTwoMapperIDF extends Mapper< Object, InvertedDocumentFrequency,
    Text, StringDoubleValue> {


  private final Text outputKeyIDF = new Text();
  private final StringDoubleValue outputValueIDF = new StringDoubleValue();

  public void map(Object key, InvertedDocumentFrequency value, Context context)
      throws IOException, InterruptedException {

    outputKeyIDF.set(value.word);
    outputValueIDF.set("-1", value.valueIDF);

    context.write(outputKeyIDF, outputValueIDF);

  }

}
