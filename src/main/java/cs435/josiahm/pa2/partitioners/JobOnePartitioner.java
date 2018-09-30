package cs435.josiahm.pa2.partitioners;

import cs435.josiahm.pa2.writableComparables.JobOneKey;
import cs435.josiahm.pa2.writableComparables.StringDoubleValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Splits the outputs of JobOneMapper to the correct reducers. Words and Ids to one reducer
 * and the output of the TF for a ID to another
 */
public class JobOnePartitioner extends Partitioner<JobOneKey, StringDoubleValue> {


  /**
   * Splits the outputs of JobOneMapper to the correct reducers. Words and Ids to one reducer
   * and the output of the TF for a ID to another
   * @param key the key of the pair
   * @param value the value of the pair
   * @param numReduceTasks the number of reducers in the jobe
   * @return which reducer to go to
   */
  @Override
  public int getPartition(JobOneKey key, StringDoubleValue value, int numReduceTasks) {

    String job = key.reducer;

    switch (job) {
      case "IDF":
        return 0;
      case "TF":
        return getReducerNumber(key, numReduceTasks, 1);
      default:
        return 1;
    }
  }

  /**
   * Selects the next reducer
   * @param key The doc id
   * @param numReduceTasks number of reducers to mod by
   * @param range the minimum number the reducer can be
   * @return the reducer ID goes to
   */
  private int getReducerNumber(JobOneKey key, int numReduceTasks, int range) {
    // Get the hashCode
    int rt = key.hashCode() % numReduceTasks;

    // If hash code is the IDF reducer
    if(rt < range){
      rt += range;
    }

    return rt;
  }

}
