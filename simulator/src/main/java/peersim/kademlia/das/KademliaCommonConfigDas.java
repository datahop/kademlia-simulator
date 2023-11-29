package peersim.kademlia.das;

import peersim.kademlia.KademliaCommonConfig;

/**
 * Fixed Parameters for the DAS protocol.
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class KademliaCommonConfigDas {

  public static int ALPHA = 5; // number of simultaneous lookup messages

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

  public static int MAX_SAMPLING_FAILED = 0;

  public static int PARCEL_SIZE = 128;
  /**
   * Size of a node record (a single neighbor information returned alongside samples in
   * GET_SAMPLE_RESPONSE) in Mbits - I used ENR size for this, which is 300 bytes
   */
  public static double NODE_RECORD_SIZE = 0.0024;

  /** Size of a sample in Mbits - each cell contains 512 B of data + 48 B KZG commitment */
  public static double SAMPLE_SIZE = 0.00448;

  /** Number of samples returned by a single node */
  public static int MAX_SAMPLES_RETURNED = 1000;

  public static int MAX_NODES_RETURNED = KademliaCommonConfig.K;

  /** Number of max hops during a sampling operation */
  public static int MAX_HOPS = 5000;

  /** Default upload bandwith of a validator in Mbits/sec */
  public static int VALIDATOR_UPLOAD_RATE = 1000;

  /** Default upload bandwith of a non-validator in Mbits/sec */
  public static int NON_VALIDATOR_UPLOAD_RATE = 100;

  public static int BUILDER_UPLOAD_RATE = 10000;

  public static int VALIDATOR_DEADLINE = 4000;
  public static int RANDOM_SAMPLING_DEADLINE = 12000;

  public static int aggressiveness_step = 1;
  public static int multiplyRadiusLimit = 0;

  public static int validatorsSize = 0;
  public static int networkSize = 0;

  public static long TTL = 30000;
}
