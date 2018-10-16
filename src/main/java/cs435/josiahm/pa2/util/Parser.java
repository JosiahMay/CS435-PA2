package cs435.josiahm.pa2.util;

import java.util.StringTokenizer;

/**
 * Base class for all Parser classes
 */
public abstract class Parser {

  /**
   * The id of the article
   */
  public String id = null;
  /**
   * The tokenizer the parser uses
   */
  protected StringTokenizer wordsToTokenize = null;

  /**
   * How to set the tokenizer
   * @param entry the value to tokenize
   * @return if the tokenizer was set
   */
  public abstract boolean set(String entry);

  /**
   * Checks if there are more wordsToTokenize to parse
   * @return if there is more wordsToTokenize in the tokenizer
   */
  public boolean hasMoreTokens(){
    return wordsToTokenize.hasMoreTokens();
  }

  /**
   * Get the next word in the article,
   * @return the next word, lower case and only alpha numeric characters
   */
  public String nextToken(){
    return wordsToTokenize.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
  }

}
