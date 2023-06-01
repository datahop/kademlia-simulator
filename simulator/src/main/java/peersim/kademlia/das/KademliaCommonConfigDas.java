package peersim.kademlia.das;

/**
 * Fixed Parameters for the DAS protocol.
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class KademliaCommonConfigDas {

  public static int ALPHA = 3; // number of simultaneous lookup messages

  /** Different ways of mapping samples to DHT keyspace */
  public static int SAMPLE_MAPPING_RANDOM = 1;

  public static int SAMPLE_MAPPING_REGION_BASED = 2;
  public static int MAPPING_FN = SAMPLE_MAPPING_REGION_BASED;

  /** Number of copies of each sample stored in the network */
  public static int NUM_SAMPLE_COPIES_PER_PEER = 2;

  /** Block matrix dimension */
  public static int BLOCK_DIM_SIZE = 10;

  /** Number of samples retrieved for the random sampling */
  public static int N_SAMPLES = 75;

  /** Number of samples returned by a single node */
  public static int MAX_SAMPLES_RETURNED = 1000;

  public static int MAX_NODES_RETURNED = 15;

  /** Number of max hops during a sampling operation */
  public static int MAX_HOPS = 5000;
}
