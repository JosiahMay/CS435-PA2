package cs435.josiahm.pa2.util;

/**
 * Generic class for a tuple class
 * @param <L> A class that implements comparable
 * @param <R>A class that implements comparable
 */
public class Tuple<L extends Comparable<? super L>, R extends Comparable<? super R>> {

  public final L left;
  public final R right;

  /**
   * Constructs the tuple
   * @param left the left value
   * @param right the right value
   */
  public Tuple(L left, R right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public int hashCode() { return (left.hashCode() << 2) ^ right.hashCode(); }

  /**
   * Checks if the left and right value of two tuples are the same
   * @param o the object to compare
   * @return if the tuples are the same
   */
  @Override
  public boolean equals(Object o) {
    // If object not tuple return false
    if (!(o instanceof Tuple)) return false;
    // Check if the right and left are the same
    Tuple obj = (Tuple) o;
    return this.left.equals(obj.left) &&
        this.right.equals(obj.right);
  }

}
