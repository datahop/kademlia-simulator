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

  public static int FINDMODE = 0; // find mode: 0 find by node id / 1 find by distance to node

  public static int AD_LIFE_TIME = 900000; // life time of ads the topic table

  public static int TTNBUCKETS = 10;
  public static int STNBUCKETS = 16;

  public static int MAX_TOPIC_REPLY = 10; // the number of nodes we get per registrar
  public static int SLOT = 1000;
  public static int REG_TIMEOUT = AD_LIFE_TIME;
  public static int REG_WINDOW = 10000;
  public static int SEARCH_REFRESH = 0;
  public static int TICKET_REFRESH = 0;
  public static int TICKET_NEIGHBOURS = 0;
  public static int TICKET_BUCKET_SIZE = 3;
  public static int SEARCH_BUCKET_SIZE = 3;
  public static int TICKET_TABLE_REPLACEMENTS = 0;
  public static int SEARCH_TABLE_REPLACEMENTS = 0;
  public static int TICKET_REMOVE_AFTER_REG = 0;

  /**
   * short information about current mspastry configuration
   *
   * @return String
   */
  public static String info() {
    return String.format("[K=%d][ALPHA=%d][BITS=%d]", K, ALPHA, BITS);
  }

  public static int MAX_REGISTRATION_RETRIES = 10; // life time of ads the topic table
  public static int ONE_UNIT_OF_TIME = 1; // smallest time value
  public static int TOPIC_PEER_LIMIT =
      30; // how many high quality nodes do we want to find for each topic; after this number we
  // stop
  public static int MAX_SEARCH_HOPS =
      50; // how many nodes we can query during a lookup operation; after this number we stop
}
