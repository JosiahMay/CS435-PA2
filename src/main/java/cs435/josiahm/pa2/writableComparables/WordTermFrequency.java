package cs435.josiahm.pa2.writableComparables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Reads the TF values from the file in Job 1
 */
public class WordTermFrequency implements WritableComparable<WordTermFrequency>{

  /**
   * The article ID of the word
   */
  public String id;
  /**
   * The word from the article
   */
  public String word;
  /**
   * The term frequency value for the word in the article
   */
  public Double tfValue;

  /**
   * Writes the state values to hadoop file system
   * @param dataOutput DataOutput to serialize this object into.
   * @throws IOException Invalid writes
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {

    dataOutput.writeUTF(id);
    dataOutput.writeUTF(word);
    dataOutput.writeDouble(tfValue);

  }

  /**
   * Reads the info from hadoop file system to fill in the values
   * @param dataInput DataInput to deseriablize this object from.
   * @throws IOException Invalid reads
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {

    this.id = dataInput.readUTF();
    this.word = dataInput.readUTF();
    this.tfValue = dataInput.readDouble();

  }

  /**
   * The hash code of the combined hashes of ID, word, and tfValue
   * @return hash code of the key
   */
  @Override
  public int hashCode(){
    Long hash = (long) (id.hashCode() + word.hashCode() + tfValue.hashCode());
    return hash.hashCode();
  }

  /**
   * Compare if two WordTermFrequency's have the same id, then word, and finally TF value
   * @param o other WordTermFrequency to check
   * @return if the ID, word, and TF value are the same
   */
  @Override
  public boolean equals(Object o) {
    boolean isEqual = false;
    if (o instanceof WordTermFrequency) {
      WordTermFrequency obj = (WordTermFrequency) o;
      isEqual = isEqual(obj);


    }
    return isEqual;
  }

  /**
   * Checks if the all the values for the two WordTermFrequency are the same
   * @param obj the other object to check
   * @return if the all the values for the two WordTermFrequency are the same
   */
  private boolean isEqual( WordTermFrequency obj) {
    // Check if same ID
    if (this.id.equals(obj.id)) {
      // Check if the word is the same
      if(this.word.equals(obj.word)){
        // Check if the TF value is the same
        return tfValue.equals(obj.tfValue);
      }
    }
    return false;
  }

  /**
   * Compares the WordTermFrequency first by ID, then word, and finally the TF value.
   * @param o the JobOneKey to compare to
   * @return the ascending  order of ID, word, and TF value
   */
  @Override
  public int compareTo(WordTermFrequency o){
    int compare = this.id.compareTo(o.id);
    if(compare == 0){
      compare = this.word.compareTo(o.word);
    }
    if(compare == 0){
      compare = this.tfValue.compareTo(o.tfValue);
    }
    return compare;
  }

  @Override
  public String toString(){
    return id + "\t" + word + "\t" + tfValue;
  }
}
