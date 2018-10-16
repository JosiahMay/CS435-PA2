package cs435.josiahm.pa2.util;

import java.util.StringTokenizer;

/**
 * Passer for a single sentence string
 */
public class SentenceParser extends Parser{

  /**
   * Sets up the tokenizer for the sentence
   * @param entry the sentence to tokenize
   * @return if the tokenizer is set up
   */
  @Override
  public boolean set(String entry) {

    wordsToTokenize = new StringTokenizer(entry);
    return true;
  }
}
