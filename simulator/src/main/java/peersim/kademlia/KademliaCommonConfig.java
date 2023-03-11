package peersim.kademlia;

/**
 * Fixed Parameters of a kademlia network. They have a default value and can be configured at
 * startup of the network, once only.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaCommonConfig {

  public static int BITS = 256; // length of Id

  public static int K = 16; // dimension of k-buckets
  public static int ALPHA = 3; // number of simultaneous lookup messages

  public static int NBUCKETS = 256; // number of buckets in the routing table

  public static int MAXREPLACEMENT = 10; // number of items in the replacement list for each bucket

  public static int FINDMODE = 1; // find mode: 0 find by node id / 1 find by distance to node

  /**
   * short information about current mspastry configuration
   *
   * @return String
   */
  public static String info() {
    return String.format("[K=%d][ALPHA=%d][BITS=%d]", K, ALPHA, BITS);
  }
}
