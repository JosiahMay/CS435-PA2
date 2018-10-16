package cs435.josiahm.pa2.writableComparables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Reads the IDF values from the file in Job 1
 */
public class InvertedDocumentFrequency implements WritableComparable<InvertedDocumentFrequency> {

  /**
   * The word
   */
  public String word;
  /**
   * The Inverted Document Frequency value for the word
   */
  public Double valueIDF;

  /**
   * Set word and it's IDF value
   * @param word the word
   * @param valueIDF the IDF value
   */
  public void set(String word, Double valueIDF){
    this.word = word;
    this.valueIDF = valueIDF;
  }

  /**
   * Writes the state values to hadoop file system
   * @param dataOutput DataOutput to serialize this object into.
   * @throws IOException Invalid writes
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(word);
    dataOutput.writeDouble(valueIDF);
  }

  /**
   * Reads the info from hadoop file system to fill in the values
   * @param dataInput DataInput to deseriablize this object from.
   * @throws IOException Invalid reads
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {

    this.word = dataInput.readUTF();
    this.valueIDF = dataInput.readDouble();

  }

  /**
   * The hash code of the InvertedDocumentFrequency
   * @return hash code of the InvertedDocumentFrequency
   */
  @Override

  public int hashCode(){
    Long hash = (long) (word.hashCode() + valueIDF.hashCode());
    return hash.hashCode();
  }

  /**
   * Compare if two InvertedDocumentFrequency's have the same word, and IDF value
   * @param o other InvertedDocumentFrequency to check
   * @return if the words and IDF values are the same are the same
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof InvertedDocumentFrequency) {
      InvertedDocumentFrequency obj = (InvertedDocumentFrequency) o;
      // Check if same word
      if (this.word.equals(obj.word)) {
        return this.valueIDF.equals(obj.valueIDF);
      }
    }
    return false;
  }

  /**
   * Compares two InvertedDocumentFrequency objects by there IDF value, then their word
   * @param o the other InvertedDocumentFrequency
   * @return he ascending order of IDF value, and word
   */
  @Override
  public int compareTo(InvertedDocumentFrequency o) {
    int compare = this.valueIDF.compareTo(o.valueIDF);
    if(compare == 0){
      compare = this.word.compareTo(o.word);
    }
    return compare;
  }

  @Override
  public String toString(){
    return word + "\t" + valueIDF;
  }
}
