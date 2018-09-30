package cs435.josiahm.pa2.writableComparables;

import com.sun.istack.NotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class is used as a composite key for the first job to be used by the partitioner.
 */
public class JobOneKey implements WritableComparable<JobOneKey> {

  /**
   * The value of the key
   */
  public String key;
  /**
   * The reducer the key is being sent to
   */
  public String reducer;

  /**
   * Sets the key and reducer
   * @param key the value of the key
   * @param reducer the reducer to send the key to
   */
  public void set(String key, String reducer) {
    this.key = key;
    this.reducer = reducer;
  }

  /**
   * Set the reducer
   * @param reducer the reducer to send the key to
   */
  public void setReducer(String reducer){
    this.set(this.key, reducer);
  }

  /**
   * Set the value
   * @param key the value of the key
   */
  public void setKey(String key) {
    this.set(key, this.reducer);
  }

  /**
   * Writes the values to hadoop file system
   * @param dataOutput DataOuput to serialize this object into.
   * @throws IOException Invalid writes
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(key);
    dataOutput.writeUTF(reducer);
  }

  /**
   * Reads the info from hadoop file system to fill in the values
   * @param dataInput DataInput to deseriablize this object from.
   * @throws IOException Invalid reads
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {

    this.key = dataInput.readUTF();
    this.reducer = dataInput.readUTF();
  }

  /**
   * The hash code of the key
   * @return hash code of the key
   */
  @Override
  public int hashCode(){
    return key.hashCode();
  }


  /**
   * Compare if two JobOneKey's have the same key
   * The question should not matter because the partitioner should have already split the
   * JobOneKey's to the correct reducers
   * @param o other JobOneKey to check
   * @return if the key's are the same
   */
  @Override
  public boolean equals(Object o){
    if(o instanceof JobOneKey){
      JobOneKey obj = (JobOneKey)o;

      return this.key.equals(obj.key);
    }
    return false;
  }


  /**
   *
   Compares the key of one JobOneKey to another.
   * @param o the JobOneKey to compare to
   * @return the default string compare value of the two key
   */
  @Override
  public int compareTo(JobOneKey o) {
    if(o == null)
      throw new NullPointerException();
    return this.key.compareTo(o.key);
  }

}
