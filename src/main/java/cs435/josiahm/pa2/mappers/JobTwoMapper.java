package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.InvertedDocumentFrequency;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobTwoMapper extends Mapper< Object, WordTermFrequency, WordTermFrequency, NullWritable> {

  private NullWritable nullOutput;
  private HashMap<String, Double> idfValues = new HashMap<>();

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

    for (URI file : idfFiles) {
      Path path = new Path(file.getPath());

      if (hdfs.exists(path)) {

        BufferedReader rdr = new BufferedReader(new InputStreamReader(hdfs.open(path)));

        String line = null;
        while ((line = rdr.readLine()) != null) {
          processWord(line);
        }
        rdr.close();

      } else {
        throw new FileNotFoundException(file.getPath());
      }

    }

  }

  private void processWord(String line) {
    String[] parts = line.split("\t");
    String word = parts[0];
    Double value = Double.parseDouble(parts[1]);
    idfValues.put(word, value);
  }


  public void map(Object key, WordTermFrequency value, Context context)
      throws IOException, InterruptedException {

    if(idfValues.containsKey(value.word)){
      value.tfValue = value.tfValue * idfValues.get(value.word);
      context.write(value, nullOutput);
    } else {
      throw new RuntimeException("missing words" + value.toString());
    }
  }

}
