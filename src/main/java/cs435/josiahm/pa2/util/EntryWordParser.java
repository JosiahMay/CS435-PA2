package cs435.josiahm.pa2.util;

import java.util.StringTokenizer;

/**
 * Takes a wikipedia article and splits it into parts and creates a tokenizer to get all the wordsToTokenize
 * this could have been done by WritableComparable
 */
public class EntryWordParser extends Parser {

  /**
   * Sets the id and tokenizer for article
   * @param entry article to parse
   * @return if the DOCID has an article
   */
  public boolean set(String entry){
    String[] parts = entry.split("<====>");
    // If the DOCID does not have 3 part it is not a complete article
    if(parts.length != 3)
      return false;

    id = parts[1];
    wordsToTokenize = new StringTokenizer(parts[2]);
    return true;
  }

}

