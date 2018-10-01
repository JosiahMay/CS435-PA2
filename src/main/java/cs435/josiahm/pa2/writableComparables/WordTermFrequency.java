package cs435.josiahm.pa2.writableComparables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class WordTermFrequency implements WritableComparable<WordTermFrequency>{

  public String id;
  public String word;
  public Double tfValue;



  @Override
  public int compareTo(WordTermFrequency o) {
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
  public void write(DataOutput dataOutput) throws IOException {

    dataOutput.writeUTF(id);
    dataOutput.writeUTF(word);
    dataOutput.writeDouble(tfValue);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

    this.id = dataInput.readUTF();
    this.word = dataInput.readUTF();
    this.tfValue = dataInput.readDouble();

  }

  @Override
  public String toString(){
    return id + "\t" + word + "\t" + tfValue;
  }

  /**
   * The hash code of the key
   * @return hash code of the key
   */
  @Override
  public int hashCode(){
    return word.hashCode();
  }


  /**
   * Compare if two StringDoubleValue's have the same id The question should not matter because the
   * partitioner should have already split the StringDoubleValue's to the correct reducers
   *
   * @param o other JobOneKey to check
   * @return if the id's are the same
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof WordTermFrequency) {
      WordTermFrequency obj = (WordTermFrequency) o;
      // Check if same ID
      if (this.id.equals(obj.id)) {
        if(this.word.equals(obj.word)){
          return this.tfValue.equals(obj.tfValue);
        }
      }
    }
    return false;
  }

}
