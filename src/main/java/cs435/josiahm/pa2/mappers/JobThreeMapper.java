package cs435.josiahm.pa2.mappers;

import cs435.josiahm.pa2.writableComparables.WordTermFrequency;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobThreeMapper extends Mapper< Object, Text, Text, Text>  {

  private HashMap<String, HashMap<String,Double>> idfTfValues = new HashMap<>();

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
          processIDFtf(line);
        }
        rdr.close();

      } else {
        throw new FileNotFoundException(file.getPath());
      }

    }
  }

  private void processIDFtf(String line) {

    String[] parts = line.split("\t");

    if(parts.length != 3){
      throw new RuntimeException("IDF.TF line invalid: " + line);
    }

    String id = parts[0];
    String word = parts[1];
    Double value = Double.parseDouble(parts[2]);

    if(idfTfValues.containsKey(id)){
      idfTfValues.get(id).put(word, value);
    } else {
      HashMap<String, Double> words = new HashMap<>();
      words.put(word, value);
      idfTfValues.put(id, words);
    }


  }

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

    if(!validArticle(value)){
      return;
    }

  }

  private boolean validArticle(Text value) {

    boolean rt = true;

    // If string has no data
    if (value.toString().isEmpty()) {
      rt = false;
    }


    return rt;
  }

}
