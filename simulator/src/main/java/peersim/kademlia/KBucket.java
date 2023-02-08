package peersim.kademlia;

import java.math.BigInteger;
import java.util.TreeMap;
import peersim.core.CommonState;

/**
 * This class implements a kademlia k-bucket. Function for the management of the neighbours update
 * are also implemented
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KBucket implements Cloneable {

  // k-bucket array
  protected TreeMap<BigInteger, Long> neighbours = null;

  // empty costructor
  public KBucket() {
    neighbours = new TreeMap<BigInteger, Long>();
  }

  // add a neighbour to this k-bucket
  public boolean addNeighbour(BigInteger node) {
    long time = CommonState.getTime();
    if (neighbours.size() < KademliaCommonConfig.K) { // k-bucket isn't full
      neighbours.put(node, time); // add neighbour to the tail of the list
      return true;
    }
    return false;
  }

  // remove a neighbour from this k-bucket
  public void removeNeighbour(BigInteger node) {
    neighbours.remove(node);
  }

  public Object clone() {
    KBucket dolly = new KBucket();
    for (BigInteger node : neighbours.keySet()) {
      dolly.neighbours.put(new BigInteger(node.toByteArray()), 0l);
    }
    return dolly;
  }

  public String toString() {
    String res = "{\n";

    for (BigInteger node : neighbours.keySet()) {
      res += node + "\n";
    }

    return res + "}";
  }
}
