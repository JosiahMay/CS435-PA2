package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.InvertedDocumentFrequency;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobTwoMapperTF extends Mapper< Object, WordTermFrequency, WordTermFrequency, NullWritable> {

  private NullWritable nullOutput;

  private final HashMap<String, Double> idfValues = new HashMap<>();

  @Override
  /**
   * Sets the reducers for the two possible outputs
   * @param context were to write for HDFS
   * @throws IOException unable to write to HDFS
   * @throws InterruptedException program interrupted
   */
  protected void setup(Context context) throws IOException, InterruptedException {

    // Get files from the cache
    FileSystem hdfs = FileSystem.get(context.getConfiguration());
    URI[] idfFiles = context.getCacheFiles();

    InvertedDocumentFrequency wordIDF = new InvertedDocumentFrequency();


    for (URI file : idfFiles) {
      Path path = new Path(idfFiles[0].getPath().toString());

      if (hdfs.exists(path)) {
        FSDataInputStream reader = hdfs.open(path);

        boolean eof = false;
        while (!eof) {
          try {
            wordIDF.readFields(reader);
            idfValues.put(wordIDF.word, wordIDF.valueIDF);
          } catch (EOFException e){
            eof = true;
          }

        }
        reader.close();


      } else {
        throw new FileNotFoundException(file.getPath());
      }

    }
  }



  public void map(Object key, WordTermFrequency value, Context context)
      throws IOException, InterruptedException {

    throw new RuntimeException(value.toString());
    //value.tfValue = value.tfValue * idfValues.get(value.word);
    //context.write(value, nullOutput);


  }

}
