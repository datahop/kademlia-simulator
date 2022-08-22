package peersim.kademlia;

/**
 * Fixed Parameters of a kademlia network. They have a default value and can be configured at
 * startup of the network, once only.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaCommonConfig {

  public static int BITS = 160; // length of Id (default is 160)

  public static int K = 20; // dimension of k-buckets (default is 20)
  public static int ALPHA = 3; // number of simultaneous lookup (default is 3)

  /**
   * short information about current mspastry configuration
   *
   * @return String
   */
  public static String info() {
    return String.format("[K=%d][ALPHA=%d][BITS=%d]", K, ALPHA, BITS);
  }
}
