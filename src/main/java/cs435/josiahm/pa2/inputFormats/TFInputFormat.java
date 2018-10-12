package cs435.josiahm.pa2.inputFormats;

import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TFInputFormat extends FileInputFormat<Object, WordTermFrequency> {



  @Override
  public RecordReader<Object, WordTermFrequency> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    TFRecordReader recordReader = new TFRecordReader();

    recordReader.initialize(inputSplit, taskAttemptContext);

    return recordReader;
  }

}
