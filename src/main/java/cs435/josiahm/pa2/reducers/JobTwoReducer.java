package cs435.josiahm.pa2.reducers;

import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobTwoReducer extends Reducer<Text, StringDoubleValue, Text, DoubleWritable> {

  private final Text outputKey = new Text();
  private final DoubleWritable outputValue = new DoubleWritable();
  private final HashMap<String, Double> ids = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<StringDoubleValue> values, Context context)
      throws IOException, InterruptedException {

    Double idfValue = new Double(0);


    for (StringDoubleValue item: values) {

      if(item.id != "-1")
      {
        ids.put(item.id, item.value);
      } else {
        idfValue = item.value;
      }

    }

    if(idfValue == 0)
    {
      throw new RuntimeException("The IDF value for " + key.toString() + " is 0" );
    }

    for (Entry<String, Double> item: ids.entrySet()) {
      outputKey.set(item.getKey() + key.toString());
      outputValue.set(item.getValue());
      context.write(outputKey, outputValue);
    }

    ids.clear();
  }

}
