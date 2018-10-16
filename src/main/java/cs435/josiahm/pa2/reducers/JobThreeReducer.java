package cs435.josiahm.pa2.reducers;

import cs435.josiahm.pa2.util.ComparatorTupleLeft;
import cs435.josiahm.pa2.util.ComparatorTupleRight;
import cs435.josiahm.pa2.util.SentenceParser;
import cs435.josiahm.pa2.util.Tuple;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Calculates the top 3 sentences per article from the top 5 words in the sentence using their
 * IDF*TF values
 */
public class JobThreeReducer extends Reducer<Text, StringDoubleValue, Text, Text> {


  private final HashMap<String,Double> idfTfValues = new HashMap<>();
  private final SentenceParser entry = new SentenceParser();
  private final TreeSet<Tuple<Double, String>> sentenceWords =
      new TreeSet<>(new ComparatorTupleLeft());
  private final TreeSet<Tuple<Double, Double>> sentenceScores =
      new TreeSet<>(new ComparatorTupleLeft());
  private final TreeSet<Tuple<Double, Double>> sortedSentenceOrder =
      new TreeSet<>(new ComparatorTupleRight<>());
  private final HashMap<Double, String> sentences = new HashMap<>();

  private final Text outputKey = new Text();
  private final Text outputValue = new Text();


  protected void reduce(Text key, Iterable<StringDoubleValue> values, Context context)
      throws IOException, InterruptedException {

    boolean foundArticle = false; // Found at least one sentence

    for (StringDoubleValue item : values) {

      String[] parts = item.id.split("<====>"); // check for sentence

      // Found sentence
      if (parts.length == 2) {
        foundArticle = true;
        sentences.put(item.value, parts[1]);
      } else {
        // Found a term value
        idfTfValues.put(item.id, item.value);
      }
    }

    // If the ID has any sentences run logic
    if (foundArticle) {
      findTopNSentences();
      writeResults(key, context);
    }

    // Clear containers for next ID
    sentences.clear();
    sentenceScores.clear();
    sortedSentenceOrder.clear();
    idfTfValues.clear();

  }


  private void writeResults(Text id, Context context) throws IOException, InterruptedException {
    // Set the sentence DOCID
    outputKey.set(id);
    // Sort by order in article
    sortedSentenceOrder.addAll(sentenceScores);

    // Loop through and write the sentence
    Iterator<Tuple<Double, Double>> sentencesID = sortedSentenceOrder.iterator();
    String summary = "";

    while(sentencesID.hasNext()){
      summary += sentences.get(sentencesID.next().right).trim() + ". ";

    }

    outputValue.set(summary);
    context.write(outputKey, outputValue);

  }

  /**
   * Finds the top 3 sentences for the ID
   */
  private void findTopNSentences() {

    // Look through each sentence
    for (Entry<Double, String> sentence :sentences.entrySet()) {
      // Set parser for sentence
      entry.set(sentence.getValue());
      // Find the top words for the sentence
      findTopWords();

      // Get the sum for the sentence
      Double scoreSentence = getSentenceScores();
      // Add the score and the article order to the tree
      sentenceScores.add(new Tuple<>(scoreSentence, sentence.getKey()));

      // Remove the lowest scoring sentence if more than three sentences
      if (sentenceScores.size() > 3){
        sentenceScores.remove(sentenceScores.first());
      }
    }

  }

  /**
   * Gets the sum of the IDF*TF values of sentence
   * @return the sum of the IDF*TF values
   */
  private Double getSentenceScores() {

    Double rt = 0.0;
    Iterator<Tuple<Double, String>> sentences = sentenceWords.iterator();
    // Loop through the words and get the sum of the IDF*TF value
    while(sentences.hasNext()){
      rt += sentences.next().left;
    }
    return rt;
  }

  /**
   * Finds the IDF*TF value for the top 5 words in the sentence
   */
  private void findTopWords() {

    // Clear the last sentence's words
    sentenceWords.clear();

    // Check each word in the sentence
    while (entry.hasMoreTokens()) {
      String word = entry.nextToken();
      if(!word.isEmpty()){
        sentenceWords.add(new Tuple<>(idfTfValues.get(word), word));
      }

      // If here is more the 5 words remove the one with the worst IDF*TF value
      if (sentenceWords.size() > 5){
        sentenceWords.remove(sentenceWords.first());
      }
    }
  }



}
