package peersim.kademlia;

/**
 * Fixed Parameters of a kademlia network. They have a default value and can be configured at
 * startup of the network, once only.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaCommonConfig {

  /** Length of Id */
  public static int BITS = 256;

  /** Dimension of k-buckets */
  public static int K = 16;

  /** Number of simultaneous lookup messages */
  public static int ALPHA = 3;

  /** Number of buckets in the routing table */
  public static int NBUCKETS = 256;

  /** Number of items in the replacement list for each bucket */
  public static int MAXREPLACEMENT = 10;

  /** Find mode: 0 find by node id / 1 find by distance to node */
  public static int FINDMODE = 1;

  /**
   * Provides short information about current Kademlia configuration
   *
   * @return a string containing the current configuration
   */
  public static String info() {
    return String.format("[K=%d][ALPHA=%d][BITS=%d]", K, ALPHA, BITS);
  }
}
