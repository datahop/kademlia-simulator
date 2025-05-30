package peersim.kademlia.das;

import peersim.kademlia.KademliaCommonConfig;

/**
 * Config parameters for the DAS protocol.
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class KademliaCommonConfigDas {

  public static int ALPHA = 75; // number of simultaneous lookup messages

  /** Different ways of mapping samples to DHT keyspace */
  public static int SAMPLE_MAPPING_RANDOM = 1;

  public static int SAMPLE_MAPPING_REGION_BASED = 2;
  public static int MAPPING_FN = SAMPLE_MAPPING_REGION_BASED;

  /** Number of copies of each sample stored in the network */
  public static int NUM_SAMPLE_COPIES_PER_PEER = 1;

  /** Block matrix dimension */
  public static int BLOCK_DIM_SIZE = 512;

  /** Number of samples retrieved for the random sampling */
  public static int N_SAMPLES = 75;

  public static int MAX_SAMPLING_FAILED = 3;

  public static int PARCEL_SIZE = 512;
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
  public static int VALIDATOR_UPLOAD_RATE = 10000;

  /** Default upload bandwith of a non-validator in Mbits/sec */
  public static int NON_VALIDATOR_UPLOAD_RATE = 10000;

  public static int BUILDER_UPLOAD_RATE = 10000;

  public static int VALIDATOR_DEADLINE = 4000;
  public static int RANDOM_SAMPLING_DEADLINE = 12000;

  public static int random_sampling_aggressiveness_step = 10;
  public static int row_column_sampling_aggressiveness_step = 1;
  public static int multiplyRadiusLimit = 0;

  public static int validatorsSize = 0;
  public static int networkSize = 0;

  // Builder Strategy to disseminate sample
  // 0 == All samples
  // 1 == Half of the samples
  // 2 == 2 of All samples

  public static int builderStrategy = 0;
  public static int builderRedundancy = 8;

  // Validator Strategy for sampling
  // 0 == Brute Force
  // 1 == Initial Number of sample search at each steps
  // 2 == Only research with a number of node equal to number of missing samples
  public static int validatorStrategy = 0;

  public static int validatorRowColumn = 2;

  public static long TTL = 100000;
}
