package cs435.josiahm.pa2.writableComparables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class holds a String and Double for the document ID, IDF, and TF values
 * if if is just the IDF value ID = "-1";
 */
public class StringDoubleValue implements WritableComparable<StringDoubleValue> {

  /**
   * The id od the document
   */
  public String id;
  /**
   * The value of the IDF or TF
   */
  public Double value;

  /**
   * Sets the id and value
   *
   * @param id the value of the id
   * @param value the value to send the id to
   */
  public void set(String id, Double value) {
    this.id = id;
    this.value = value;
  }

  /**
   * Set the value
   *
   * @param value the value to send the id to
   */
  public void setValue(Double value) {
    this.set(this.id, value);
  }

  /**
   * Set the value
   *
   * @param id the the id
   */
  public void setId(String id) {
    this.set(id, this.value);
  }

  /**
   * Writes the state to hadoop file system
   *
   * @param dataOutput DataOuput to serialize this object into.
   * @throws IOException Invalid writes
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(id);
    dataOutput.writeDouble(value);
  }

  /**
   * Reads the info from hadoop file system to fill in the state
   *
   * @param dataInput DataInput to deseriablize this object from.
   * @throws IOException Invalid reads
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {

    this.id = dataInput.readUTF();
    this.value = dataInput.readDouble();
  }

  /**
   * The hash code of the value
   *
   * @return hash code of the value
   */
  @Override
  public int hashCode() {
    return value.hashCode();
  }


  /**
   * Compare if two JobOneKey's have the same id The question should not matter because the
   * partitioner should have already split the JobOneKey's to the correct reducers
   *
   * @param o other JobOneKey to check
   * @return if the id's are the same
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof StringDoubleValue) {
      StringDoubleValue obj = (StringDoubleValue) o;
      // Check if same ID
      if (this.id.equals(obj.id)) {
        return this.value.equals(obj.value);
      }
    }
    return false;
  }


  /**
   * Compares the id of one JobOneKey to another.
   *
   * @param o the JobOneKey to compare to
   * @return the default string compare value of the two id
   */
  @Override
  public int compareTo(StringDoubleValue o) {
    if (o == null)
      throw new NullPointerException();
    // First compare ID
    int compare = this.id.compareTo(o.id);
    if (compare == 0) {
      // If ID == ID then compare value
      compare = this.value.compareTo(o.value);
    }

    return compare;
  }
}


