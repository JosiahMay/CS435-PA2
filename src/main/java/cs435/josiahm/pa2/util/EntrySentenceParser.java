package cs435.josiahm.pa2.util;

import java.util.StringTokenizer;

/**
 * Parser for all the sentences in an article
 */
public class EntrySentenceParser extends Parser {

  /**
   * All the sentences in the article
   */
  private String[] sentences = null;

  /**
   * Sets the id and sentences for article
   * @param entry article to parse
   * @return if the DOCID has an article
   */
  public boolean set(String entry){
    String[] parts = entry.split("<====>");
    // If the DOCID does not have 3 part it is not a complete article
    if(parts.length != 3)
      return false;

    id = parts[1];
    sentences = parts[2].split("\\.\\s");

    return true;
  }

  /**
   * Checks if there are more wordsToTokenize to parse
   * @return the number of sentences in the article
   */
  public int getNumberOfSentences(){
    return sentences.length;
  }

  /**
   * Sets the a sentences from the article to parse
   * @param sentencesNumber the index of the sentence to parse
   * @return if the sentence was set
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public boolean setSentencesToParse(int sentencesNumber) throws IndexOutOfBoundsException {
    indexInRange(sentencesNumber);
    wordsToTokenize = new StringTokenizer(sentences[sentencesNumber]);
    return true;
  }



  /**
   * Get the a sentences from the article
   * @param sentencesNumber the index of the sentence to get
   * @return if the sentence was set
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public String getSentences(int sentencesNumber) throws IndexOutOfBoundsException {
    indexInRange(sentencesNumber);
    return sentences[sentencesNumber];
  }

  /**
   * Check to see if an index in range
   * @param index the index to check
   * @throws IndexOutOfBoundsException if trying to set an index outside of the sentence array size
   */
  private void indexInRange(int index) throws IndexOutOfBoundsException {
    if(index < 0 || index > sentences.length){
      throw new IndexOutOfBoundsException("sentencesNumber: " + index
          + " out of range for array of size: " + sentences.length);
    }
  }
}
