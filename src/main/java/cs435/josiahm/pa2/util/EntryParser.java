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
   * @return if the ID has an article
   */
  public boolean set(String entry){
    String[] parts = entry.split("<====>");
    // If the ID does not have 3 part it is not a complete article
    if(parts.length != 3)
      return false;

    id = parts[1];
    words = new StringTokenizer(parts[2]);
    return true;
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
    return words.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
  }

}

