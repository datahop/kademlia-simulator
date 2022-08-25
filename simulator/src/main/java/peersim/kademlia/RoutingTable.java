package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Gives an implementation for the rounting table component of a kademlia node
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RoutingTable implements Cloneable {

  // node ID of the node
  private BigInteger nodeId = null;

  // k-buckets
  public TreeMap<Integer, KBucket> k_buckets = null;

  // ______________________________________________________________________________________________
  /** instanciates a new empty routing table with the specified size */
  public RoutingTable() {
    k_buckets = new TreeMap<Integer, KBucket>();
    // initialize k-bukets
    for (int i = 0; i <= KademliaCommonConfig.BITS; i++) {
      k_buckets.put(i, new KBucket());
    }
  }

  // add a neighbour to the correct k-bucket
  public void addNeighbour(BigInteger node) {
    // get the lenght of the longest common prefix (correspond to the correct k-bucket)
    int prefix_len = Util.distance(nodeId, node) - 1;

    // add the node to the k-bucket
    if (prefix_len >= 0) k_buckets.get(prefix_len).addNeighbour(node);
  }

  // remove a neighbour from the correct k-bucket
  public void removeNeighbour(BigInteger node) {
    // get the lenght of the longest common prefix (correspond to the correct k-bucket)
    int prefix_len = Util.distance(nodeId, node) - 1;

    // add the node to the k-bucket
    if (prefix_len >= 0) k_buckets.get(prefix_len).removeNeighbour(node);
  }

  // return the closest neighbour to a key from the correct k-bucket
  public BigInteger[] getNeighbours(final BigInteger key, final BigInteger src) {
    // resulting neighbours
    BigInteger[] result = new BigInteger[KademliaCommonConfig.K];

    // neighbour candidates
    ArrayList<BigInteger> neighbour_candidates = new ArrayList<BigInteger>();

    // get the lenght of the longest common prefix
    int prefix_len = Util.distance(nodeId, key) - 1;

    if (prefix_len < 0) return new BigInteger[] {nodeId};
    // return the k-bucket if is full
    if (k_buckets.get(prefix_len).neighbours.size() >= KademliaCommonConfig.K) {
      return k_buckets.get(prefix_len).neighbours.keySet().toArray(result);
    }

    // else get k closest node from all k-buckets
    prefix_len = 0;
    while (prefix_len < KademliaCommonConfig.BITS) {
      neighbour_candidates.addAll(k_buckets.get(prefix_len).neighbours.keySet());
      // remove source id
      neighbour_candidates.remove(src);
      prefix_len++;
    }

    // create a map (distance, node)
    TreeMap<Integer, List<BigInteger>> distance_map = new TreeMap<Integer, List<BigInteger>>();

    for (BigInteger node : neighbour_candidates) {
      if (distance_map.get(Util.distance(node, key)) == null) {
        List<BigInteger> l = new ArrayList<BigInteger>();
        l.add(node);
        distance_map.put(Util.distance(node, key), l);

      } else {
        distance_map.get(Util.distance(node, key)).add(node);
      }
    }

    List<BigInteger> bestNeighbours = new ArrayList<BigInteger>();
    for (List<BigInteger> list : distance_map.values()) {
      for (BigInteger i : list) {
        if (bestNeighbours.size() < KademliaCommonConfig.K) bestNeighbours.add(i);
        else break;
      }
    }
    if (bestNeighbours.size() < KademliaCommonConfig.K)
      result = new BigInteger[bestNeighbours.size()];

    return bestNeighbours.toArray(result);
  }

  // ______________________________________________________________________________________________
  public Object clone() {
    RoutingTable dolly = new RoutingTable();
    for (int i = 0; i < KademliaCommonConfig.BITS; i++) {
      k_buckets.put(i, new KBucket()); // (KBucket) k_buckets.get(i).clone());
    }
    return dolly;
  }

  // ______________________________________________________________________________________________
  /**
   * print a string representation of the table
   *
   * @return String
   */
  public String toString() {
    return "";
  }

  public void setNodeId(BigInteger id) {
    this.nodeId = id;
  }

  public BigInteger getNodeId() {
    return this.nodeId;
  }
  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
