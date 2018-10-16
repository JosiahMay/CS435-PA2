package cs435.josiahm.pa2.inputFormats;

import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Input file format for TF and IDF*TF files
 */
public class TFInputFormat extends FileInputFormat<Object, WordTermFrequency> {


  /**
   *
   * @param inputSplit The split to read
   * @param taskAttemptContext the information about to task
   * @return a record reader
   * @throws IOException Error reading the data
   * @throws InterruptedException program interrupted
   */
  @Override
  public RecordReader<Object, WordTermFrequency> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    // Make record reader
    TFRecordReader recordReader = new TFRecordReader();

    // Start record Reader
    recordReader.initialize(inputSplit, taskAttemptContext);

    return recordReader;
  }

}
