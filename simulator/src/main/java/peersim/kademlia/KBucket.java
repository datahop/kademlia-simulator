package peersim.kademlia;

import java.math.BigInteger;
import java.util.TreeMap;
import peersim.core.CommonState;

/**
 * This class implements a kademlia k-bucket. Functions for the management of the neighbours update
 * are also implemented.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KBucket implements Cloneable {

  // k-bucket array
  protected TreeMap<BigInteger, Long> neighbours = null;

  /** Empty constructor for initializing the k-bucket TreeMap. */
  public KBucket() {
    neighbours = new TreeMap<BigInteger, Long>();
  }

  /**
   * Add a neighbour to this k-bucket.
   *
   * @param node the neighbor to be added.
   * @return true if the neighbor is successfully added; false if the k-bucket is already full.
   */
  public boolean addNeighbour(BigInteger node) {
    long time = CommonState.getTime();
    if (neighbours.size() < KademliaCommonConfig.K) { // k-bucket isn't full
      neighbours.put(node, time); // add neighbor to the tail of the list
      return true;
    }
    return false;
  }

  /**
   * Remove a neighbour from this k-bucket.
   *
   * @param node the neighbour to be removed.
   */
  public void removeNeighbour(BigInteger node) {
    neighbours.remove(node);
  }

  /**
   * Returns a deep copy of the k-bucket object.
   *
   * @return a cloned k-bucket object.
   */
  public Object clone() {
    KBucket dolly = new KBucket();
    for (BigInteger node : neighbours.keySet()) {
      dolly.neighbours.put(new BigInteger(node.toByteArray()), 0l);
    }
    return dolly;
  }

  /**
   * Returns a string representation of the k-bucket object.
   *
   * @return a string representation of the k-bucket object.
   */
  public String toString() {
    String res = "{\n";

    for (BigInteger node : neighbours.keySet()) {
      res += node + "\n";
    }

    return res + "}";
  }
}
