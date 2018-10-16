package cs435.josiahm.pa2.inputFormats;

import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Record reader for the TF and IDF*TF files that stores all the values
 */
public class TFRecordReader extends RecordReader <Object, WordTermFrequency> {

  /**
   * Line record reader, Read to \n
   */
  private LineRecordReader lineRecordReader;
  /**
   * Key not used, could be ID but it needed to be unique
   */
  private Object key = new Object();

  /**
   * The class that holds the data
   */
  private WordTermFrequency value = new WordTermFrequency();


  /**
   * Sets the record reader for class
   * @param inputSplit the split that defines the range of records to read
   * @param taskAttemptContext the information about the task
   * @throws IOException Error reading the data
   * @throws InterruptedException program interrupted
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    lineRecordReader = new LineRecordReader();
    lineRecordReader.initialize(inputSplit, taskAttemptContext);
  }

  /**
   * Reads the next key value pair for the text
   * @return if the key value was read successfully
   * @throws IOException Error reading the data
   * @throws InterruptedException program interrupted
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // No more keys/values
    if (!lineRecordReader.nextKeyValue()) {
      return false;
    }

    // File is \t spaced
    String[] parts = lineRecordReader.getCurrentValue().toString().split("\t");

    // Should only have three parts
    if(parts.length != 3){
      return false;
    }

    try {
      Double tfValue = Double.parseDouble(parts[2]);
      value.set(parts[0], parts[1], tfValue);


    } catch (NumberFormatException e) {
      // Bad Record
      throw new IOException("Error parsing floating point value in record "
          + lineRecordReader.getCurrentValue().toString());
    }

    return true;
  }

  /**
   * Gets the current key
   * @return the current Object key
   * @throws IOException Error reading the data
   * @throws InterruptedException program interrupted
   */
  @Override
  public Object getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  /**
   * Gets the current value
   * @return the current value
   * @throws IOException Error reading the data
   * @throws InterruptedException program interrupted
   */
  @Override
  public WordTermFrequency getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  /**
   * The progress in reading the record
   * @return a number between 0.0 and 1.0 that is the fraction of the data read
   * @throws IOException Error reading the data
   * @throws InterruptedException program interrupted
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return lineRecordReader.getProgress();
  }

  /**
   * Closes the record reader
   * @throws IOException error in closing the RecordReader
   */
  @Override
  public void close() throws IOException {
    lineRecordReader.close();
  }


}
