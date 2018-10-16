package cs435.josiahm.pa2.util;

import java.util.Comparator;

/**
 * Comparator for a tuple class by the right value
 *
 * @param <L> A class that implements comparable
 * @param <R>A class that implements comparable
 */
public class ComparatorTupleRight<L extends Comparable<? super L>, R extends Comparable<? super R>>
    implements Comparator<Tuple<L, R>> {


  @Override
  public int compare(Tuple o1, Tuple o2) {
    // Compare right value first
    Integer compare = o1.right.compareTo(o2.right);
    if (compare == 0) {
      compare = o1.left.compareTo(o2.left);
    }

    return compare;
  }

}
