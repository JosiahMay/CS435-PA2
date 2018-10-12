package cs435.josiahm.pa2.inputFormats;

import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class TFRecordReader extends RecordReader <Object, WordTermFrequency> {

  private LineRecordReader lineRecordReader;
  private Object key = new Object();
  private WordTermFrequency value = new WordTermFrequency();



  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    lineRecordReader = new LineRecordReader();
    lineRecordReader.initialize(inputSplit, taskAttemptContext);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    if (!lineRecordReader.nextKeyValue()) {
      return false;
    }


    String[] parts = lineRecordReader.getCurrentValue().toString().split("\t");

    if(parts.length != 3){
      return false;
    }

    try {
      Double tfValue = Double.parseDouble(parts[2]);
      value.set(parts[0], parts[1], tfValue);


    } catch (NumberFormatException e) {
      throw new IOException("Error parsing floating point value in record "
          + lineRecordReader.getCurrentValue().toString());
    }


    return true;
  }

  @Override
  public Object getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public WordTermFrequency getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return lineRecordReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineRecordReader.close();
  }


}
