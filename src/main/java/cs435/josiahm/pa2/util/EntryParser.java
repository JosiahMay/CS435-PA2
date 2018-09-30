package cs435.josiahm.pa2.util;

import java.util.StringTokenizer;

/**
 * Takes a wikipedia article and splits it into parts and creates a tokenizer to get all the words
 * this could have been done by WritableComparable
 */
public class EntryParser {

  public String id = null;
  private StringTokenizer words = null;

  /**
   * Sets the id and tokenizer for article
   * @param entry article to parse
   */
  public void set(String entry){
    String[] parts = entry.split("<====>");
    id = parts[1];
    words = new StringTokenizer(parts[2].replaceAll("[^A-Za-z0-9]", "").toLowerCase());
  }

  /**
   * Checks if there are more words to parse
   * @return if there is more words in the tokenizer
   */
  public boolean hasMoreTokens(){
    return words.hasMoreTokens();
  }

  /**
   * Get the next word in the article,
   * @return the next word, lower case and only alpha numeric characters
   */
  public String nextToken(){
    return words.nextToken();
  }

}

