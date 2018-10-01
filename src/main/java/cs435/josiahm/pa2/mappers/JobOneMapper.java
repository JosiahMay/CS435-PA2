package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.util.EntryParser;
import cs435.josiahm.pa2.util.IdCounter.TOTALS;
import cs435.josiahm.pa2.writableComparables.JobOneKey;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobOneMapper extends Mapper< Object, Text, JobOneKey, StringDoubleValue> {

  private final JobOneKey outputKeyIDF = new JobOneKey();
  private final JobOneKey outputKeyTF = new JobOneKey();
  private final StringDoubleValue outputValueIDF = new StringDoubleValue();
  private final StringDoubleValue outputValueTF = new StringDoubleValue();

  private final TreeMap<String, Double> wordCount = new TreeMap<>();
  private final EntryParser entry = new EntryParser();

  /**
   * Sets the reducers for the two possible outputs
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  protected void setup(Context context) throws IOException, InterruptedException {
    outputKeyIDF.setReducer("IDF");
    outputKeyTF.setReducer("TF");

    outputValueIDF.set("-1", 1.0);

  }


  /**
   * Reads a wikipedia article, sends each unique word to be counted by the reducer, and
   * calculates the TF for every word in the article
   *
   * @param key not used for anything
   * @param value the wikipedia article
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    // If string has no data
    if(!isValidDocument(value)) {
      return;
    }

    // Add to the Total ID count
    context.getCounter(TOTALS.ID).increment(1);

    outputKeyTF.setKey(entry.id);
    // Go through each word
    while (entry.hasMoreTokens()) {
      processWord(context);

    }
    calculateAndWriteTF(context);

    // Clear the counts for new ID
    wordCount.clear();

  }

  /**
   * Process the word by increasing the count of the word and writing the (word, id) for IDf score
   * if the word was not seen before
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  private void processWord(Context context) throws IOException, InterruptedException {
    String word = entry.nextToken();

    // Check if there is a value
    if(!word.isEmpty())
    {
      writeIDF(context, word);
      addToCount(word);
    }
  }

  /**
   * Checks to see if the text given is a valid document ie: Not empty and has words in the article
   * @param value the text to check
   * @return true if valid, false if text is empty or there is no article
   */
  public boolean isValidDocument(Text value){
    boolean rt = true;

    // If string has no data
    if (value.toString().isEmpty()) {
      rt = false;
    }
    // Check if the ID has not article
    if(!entry.set(value.toString())) {
      rt = false;
    }

    return rt;
  }


  /**
   * Writes a word for IDF count if the word has not been seen before
   * @param context how to write to HDFS
   * @param word The word to write if it has not been seen
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  private void writeIDF(Context context, String word)
      throws IOException, InterruptedException {
    // Check if the word has not already been seen in the article
    if(!wordCount.containsKey(word))
    {
      outputKeyIDF.setKey(word);
      context.write(outputKeyIDF, outputValueIDF);
    }
  }

  /**
   * Calculates the TF for every word in the document and outputs (ID, (word, TF)) to the reducer
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  private void calculateAndWriteTF(Context context) throws IOException, InterruptedException {
    Double maxCount = getMaxCount();

    for (Entry word: wordCount.entrySet()) {
      Double tf = calculateTF(word, maxCount);
      writeTF(word, tf, context);
    }

  }

  /**
   * Finds the count of the word the was in the document the most
   * @return the count
   */
  private Double getMaxCount() {
    Double maxCount = new Double(0);

    // Find the word with the highest count
    for (Entry word: wordCount.entrySet()) {
      Double current = (Double) word.getValue();
      if(current > maxCount){
        maxCount = current;
      }
    }
    return maxCount;
  }

  /**
   * Outputs all the words and TF value for all the words in the document
   * @param word the word
   * @param tf the tf value
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  private void writeTF(Entry word, Double tf, Context context)
      throws IOException, InterruptedException {
    if(word.getKey() instanceof String)
    {
      outputValueTF.set((String) word.getKey(), tf);
      context.write(outputKeyTF, outputValueTF );
    }
  }

  /**
   * Calculates the TF value of a word given its frequency and the count of the word
   * with the highest frequency
   * @param word word to calculate the TF value
   * @param maxCount count of the word with the highest frequency
   * @return the TF value
   */
  private double calculateTF(Entry word, Double maxCount){
    Double count = 0.0;
    if(word.getValue() instanceof Double){
      count = (Double) word.getValue();
    }
    return .5 + .5 * (count/ maxCount);

  }

  /**
   * Adds a word to the word count, if the word has already been seen increment the count
   * @param word the word to count
   */
  private void addToCount(String word) {

    if(wordCount.containsKey(word)){
      // If the word has already been counted add to count
      Double count = wordCount.get(word) + 1;
      wordCount.replace(word, count);
    } else {
      // Add word to the count
      wordCount.put(word, new Double(1.0));
    }
  }

}
