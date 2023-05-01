package peersim.kademlia.das;

/**
 * Fixed Parameters for the DAS protocol.
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class KademliaCommonConfigDas {

  public static int BITS = 256; // length of Id

  public static int K = 16; // dimension of k-buckets
  public static int ALPHA = 3; // number of simultaneous lookup messages

  public static int NBUCKETS = 256; // number of buckets in the routing table

  public static int MAXREPLACEMENT = 10; // number of items in the replacement list for each bucket

  public static int FINDMODE = 1; // find mode: 0 find by node id / 1 find by distance to node

  /** Different ways of mapping samples to DHT keyspace */
  public static int SAMPLE_MAPPING_RANDOM = 1;

  public static int SAMPLE_MAPPING_REGION_BASED = 2;
  public static int MAPPING_FN = SAMPLE_MAPPING_REGION_BASED;

  /** Number of copies of each sample stored in the network */
  public static int NUM_SAMPLE_COPIES_PER_PEER = 3;

  /** Block matrix dimension */
  public static int BLOCK_DIM_SIZE = 10;

  /** Number of samples retrieved for the random sampling */
  public static int N_SAMPLES = 75;

  /** Number of samples returned by a single node */
  public static int MAX_SAMPLES_RETURNED = 15;

  /** Number of samples returned by a single node */
  public static int MAX_HOPS = 50;

  /**
   * short information about current mspastry configuration
   *
   * @return String
   */
  public static String info() {
    return String.format("[K=%d][ALPHA=%d][BITS=%d]", K, ALPHA, BITS);
  }
}
