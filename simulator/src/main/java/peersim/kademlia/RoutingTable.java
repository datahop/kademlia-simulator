package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Provides an implementation for the routing table component of a Kademlia node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RoutingTable implements Cloneable {

  /** Node ID of the node. */
  protected BigInteger nodeId = null;

  /** K-buckets. */
  protected TreeMap<Integer, KBucket> k_buckets = null;

  /** Number of k-buckets. */
  protected int nBuckets;

  /** Bucket size. */
  protected int k;

  /** Number of maximum bucket replacements. */
  protected int maxReplacements;

  /** Distance for the lowest bucket. */
  protected int bucketMinDistance;

  protected int findMode;
  /**
   * Instantiates a new routing table with the specified parameters.
   *
   * @param nBuckets the number of k-buckets
   * @param k the bucket size
   * @param maxReplacements the maximum number of bucket replacements
   */
  public RoutingTable(int nBuckets, int k, int maxReplacements) {
    // Initialize k-buckets.
    k_buckets = new TreeMap<Integer, KBucket>();

    this.nBuckets = nBuckets;

    this.k = k;

    this.maxReplacements = maxReplacements;

    bucketMinDistance = KademliaCommonConfig.BITS - nBuckets;

    this.findMode = KademliaCommonConfig.FINDMODE;

    /** Fills the k-buckets map with empty buckets. */
    for (int i = 0; i < nBuckets; i++) {
      k_buckets.put(i, new KBucket());
    }
  }

  // Add a neighbour to the correct k-bucket
  public boolean addNeighbour(BigInteger node) {
    // Add the node to the k-bucket
    return bucketAtDistance(Util.logDistance(nodeId, node)).addNeighbour(node);
  }

  // Remove a neighbour from the correct k-bucket
  public void removeNeighbour(BigInteger node) {

    // Remove the node from the k-bucket
    bucketAtDistance(Util.logDistance(nodeId, node)).removeNeighbour(node);
  }

  /**
   * Retrieves the closest neighbors to a key from the appropriate k-bucket using log distance.
   *
   * @param key the key to find neighbors for
   * @param src the source node ID
   * @return an array of BigInteger representing the closest neighbors
   */
  // Return the neighbours with a specific common prefix len
  public BigInteger[] getNeighbours(final int dist) {
    BigInteger[] result = new BigInteger[0];
    ArrayList<BigInteger> resultList = new ArrayList<BigInteger>();
    // Add neighbors at the given distance
    resultList.addAll(bucketAtDistance(dist).neighbours.keySet());

    if (resultList.size() < k && (dist + 1) <= 256) {
      // Add neighbors at the next distance

      resultList.addAll(bucketAtDistance(dist + 1).neighbours.keySet());
      // Remove excess neighbors until the size is <= k
      while (resultList.size() > k) resultList.remove(resultList.size() - 1);
    }
    // Add neighbors at the previous distance
    if (resultList.size() < k & (dist - 1) >= 0) {
      resultList.addAll(bucketAtDistance(dist - 1).neighbours.keySet());
      while (resultList.size() > k) resultList.remove(resultList.size() - 1);
    }
    return resultList.toArray(result);
  }

  /**
   * Return the closest neighbour to a key from the correct k-bucket.
   *
   * @param key The key to find the closest neighbour to.
   * @param src The source identifier to exclude from neighbour candidates.
   * @return An array of the closest neighbours.
   */
  public BigInteger[] getNeighbours(final BigInteger key, final BigInteger src) {
    // Resulting neighbours
    BigInteger[] result = new BigInteger[KademliaCommonConfig.K];

    // Neighbour candidates
    ArrayList<BigInteger> neighbour_candidates = new ArrayList<BigInteger>();

    // Get the length of the longest common prefix
    int prefix_len = Util.logDistance(nodeId, key);

    if (prefix_len < 0) return new BigInteger[] {nodeId};
    // Return the k-bucket if it is full
    if (bucketAtDistance(prefix_len).neighbours.size() >= KademliaCommonConfig.K) {
      return bucketAtDistance(prefix_len).neighbours.keySet().toArray(result);
    }

    // Else get k closest nodes from all k-buckets
    prefix_len = 0;
    while (prefix_len < KademliaCommonConfig.BITS) {
      neighbour_candidates.addAll(bucketAtDistance(prefix_len).neighbours.keySet());
      // Remove source id
      neighbour_candidates.remove(src);
      prefix_len++;
    }

    // Create a map (distance, node)
    TreeMap<Integer, List<BigInteger>> distance_map = new TreeMap<Integer, List<BigInteger>>();

    for (BigInteger node : neighbour_candidates) {
      if (distance_map.get(Util.logDistance(node, key)) == null) {
        List<BigInteger> l = new ArrayList<BigInteger>();
        l.add(node);
        distance_map.put(Util.logDistance(node, key), l);

      } else {
        distance_map.get(Util.logDistance(node, key)).add(node);
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
    RoutingTable dolly = new RoutingTable(nBuckets, k, maxReplacements);
    for (int i = 0; i < k_buckets.size(); i++) {
      k_buckets.put(i, new KBucket()); // (KBucket) k_buckets.get(i).clone());
    }
    return dolly;
  }

  // ______________________________________________________________________________________________
  /**
   * Generate a string representation of the routing table.
   *
   * @return the string representation of the routing table
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Routing table for Node ").append(nodeId).append(":\n");

    for (int i = 0; i < nBuckets; i++) {
      // Print the number of elements in each k-bucket
      //  sb.append("KBucket ").append(i);

      sb.append(k_buckets.get(i).getNeighborCount()).append("\n");

      // Uncomment to print all the elements in this k-bucket (row)
      // KBucket kBucket = k_buckets.get(i);
      // sb.append(kBucket.toString()).append("\n");
    }

    return sb.toString();
  }

  public KBucket getBucket(BigInteger node) {
    return bucketAtDistance(Util.logDistance(nodeId, node));
  }

  public int getBucketNum(BigInteger node) {
    int dist = Util.logDistance(nodeId, node);
    if (dist <= bucketMinDistance) {
      return 0;
    }
    return dist - bucketMinDistance - 1;
  }

  protected KBucket bucketAtDistance(int distance) {

    if (distance <= bucketMinDistance) {
      return k_buckets.get(0);
    }

    return k_buckets.get(distance - bucketMinDistance - 1);
  }

  public int getbucketMinDistance() {
    return bucketMinDistance;
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
