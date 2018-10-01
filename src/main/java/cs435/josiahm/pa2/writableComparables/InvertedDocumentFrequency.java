package cs435.josiahm.pa2.writableComparables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class InvertedDocumentFrequency implements WritableComparable<InvertedDocumentFrequency> {

  public String word;
  public Double valueIDF;

  public void set(String word, Double valueIDF){
    this.word = word;
    this.valueIDF = valueIDF;
  }

  @Override
  public int compareTo(InvertedDocumentFrequency o) {
    int compare = this.valueIDF.compareTo(o.valueIDF);
    if(compare == 0){
      compare = this.word.compareTo(o.word);
    }
    return compare;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(word);
    dataOutput.writeDouble(valueIDF);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

    this.word = dataInput.readUTF();
    this.valueIDF = dataInput.readDouble();

  }

  @Override
  public String toString(){
    return word + "\t" + valueIDF;
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
    if (o instanceof InvertedDocumentFrequency) {
      InvertedDocumentFrequency obj = (InvertedDocumentFrequency) o;
      // Check if same ID
      if (this.word.equals(obj.word)) {
        return this.valueIDF.equals(obj.valueIDF);
      }
    }
    return false;
  }
}
