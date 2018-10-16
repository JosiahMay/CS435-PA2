package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.drivers.JobOneDriver;
import cs435.josiahm.pa2.util.EntryWordParser;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobOneMapperTF extends Mapper< Object, Text, Text, StringDoubleValue> {

  private final Text outputKeyTF = new Text();
  private final StringDoubleValue outputValueTF = new StringDoubleValue();

  private final TreeMap<String, Double> wordCount = new TreeMap<>();
  private final EntryWordParser entry = new EntryWordParser();



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

    // Add to the Total DOCID count
    context.getCounter(JobOneDriver.DocumentsCount.NUMDOCS).increment(1);

    outputKeyTF.set(entry.id);
    // Go through each word
    while (entry.hasMoreTokens()) {
      processWord(context);

    }
    calculateAndWriteTF(context);

    // Clear the counts for new DOCID
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
      addToCount(word);
    }
  }

  /**
   * Checks to see if the text given is a valid document ie: Not empty and has wordsToTokenize in the article
   * @param value the text to check
   * @return true if valid, false if text is empty or there is no article
   */
  public boolean isValidDocument(Text value){
    boolean rt = true;

    // If string has no data
    if (value.toString().isEmpty()) {
      rt = false;
    }
    // Check if the DOCID has no article
    if(!entry.set(value.toString())) {
      rt = false;
    }

    return rt;
  }



  /**
   * Calculates the TF for every word in the document and outputs (DOCID, (word, TF)) to the reducer
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
   * Outputs all the wordsToTokenize and TF value for all the wordsToTokenize in the document
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
