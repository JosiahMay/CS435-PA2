package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.util.EntryWordParser;
import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for finding the IDF value it finds all the words in the article then sends only one copy
 * to the reducer
 */
public class JobOneMapperIDF extends Mapper< Object, Text, Text, IntWritable> {

  /**
   * Output key the term
   */
  private final Text outputKeyTF = new Text();
  /**
   * The count of ID with a term for the reducer
   */
  private final IntWritable outputValueTF = new IntWritable(1);

  /**
   * Save one copy of all words
   */
  private final TreeSet<String> words = new TreeSet<>();
  /**
   * Parser for the article
   */
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

    // Go through each word
    while (entry.hasMoreTokens()) {
      processWord(context);

    }
    writeIDF(context);

    // Clear the counts for new DOCID
    words.clear();

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
      words.add(word);
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
   * Sends one copy of every word in the reducer
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  private void writeIDF( Context context) throws IOException, InterruptedException {

    for (String word: words) {
      outputKeyTF.set(word);
      context.write(outputKeyTF, outputValueTF);
    }

  }




}
