package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.Message;
import peersim.kademlia.UniformRandomGenerator;

/**
 * This control generates samples every 5 min that are stored in a single node (builder) and starts
 * random sampling from the rest of the nodes In parallel, random lookups are started to start
 * discovering nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGeneratorSample implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_KADPROT = "kadprotocol";

  private static final String PAR_DASPROT = "dasprotocol";

  /** MSPastry Protocol ID to act */
  private final int kadpid, daspid;

  /** Mapping function for samples */
  final String PAR_MAP_FN = "mapping_fn";

  /** Number of sample copies stored per node */
  final String PAR_NUM_COPIES = "sample_copy_per_node";

  final String PAR_BLK_DIM_SIZE = "block_dim_size";

  int mapfn;

  Block b;
  private long ID_GENERATOR = 0;

  private boolean first = true, second = true;
  // ______________________________________________________________________________________________
  public TrafficGeneratorSample(String prefix) {
    kadpid = Configuration.getPid(prefix + "." + PAR_KADPROT);
    daspid = Configuration.getPid(prefix + "." + PAR_DASPROT);

    KademliaCommonConfigDas.MAPPING_FN = Configuration.getInt(prefix + "." + PAR_MAP_FN);
    KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER =
        Configuration.getInt(prefix + "." + PAR_NUM_COPIES);
    KademliaCommonConfigDas.BLOCK_DIM_SIZE =
        Configuration.getInt(
            prefix + "." + PAR_BLK_DIM_SIZE, KademliaCommonConfigDas.BLOCK_DIM_SIZE);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateNewBlockMessage(Block b) {

    Message m = Message.makeInitNewBlock(b);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateNewSampleMessage(Sample s) {

    Message m = Message.makeInitGetSample(s.getId());
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage() {

    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    BigInteger id = urg.generate();

    Message m = Message.makeInitFindNode(id);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {
    if (first) {
      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        if (n.isUp()) {
          for (int j = 0; j < 3; j++) {
            int time = CommonState.r.nextInt(300000);
            Node start = Network.get(i);
            Message lookup = generateFindNodeMessage();
            int kadpid = n.getKademliaProtocol().getProtocolID();
            EDSimulator.add(time, lookup, start, kadpid);
          }
        }
      }
    } else {
      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        if (n.isUp()) {
          int time = CommonState.r.nextInt(300000);
          Node start = Network.get(i);
          Message lookup = generateFindNodeMessage();
          int kadpid = n.getKademliaProtocol().getProtocolID();
          EDSimulator.add(time, lookup, start, kadpid);
        }
      }

      Block b = new Block(KademliaCommonConfigDas.BLOCK_DIM_SIZE, ID_GENERATOR);
      BigInteger radius = b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
      int samplesWithinRegion = 0; // samples that are within at least one node's region
      int totalSamples = 0;
      while (b.hasNext()) {
        Sample s = b.next();
        boolean inRegion = false;

        for (int i = 0; i < Network.size(); i++) {
          Node n = Network.get(i);
          DASProtocol dasProt = (DASProtocol) (n.getDASProtocol());

          if (n.isUp() && s.isInRegion(dasProt.getKademliaId(), radius)) {
            totalSamples++;
            int daspid = n.getDASProtocol().getDASProtocolID();
            EDSimulator.add(0, generateNewSampleMessage(s), n, daspid);
            if (inRegion == false) {
              samplesWithinRegion++;
              inRegion = true;
            }
          }
        }
      }

      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        Block bis = (Block) b.clone();
        int daspid = n.getDASProtocol().getDASProtocolID();
        EDSimulator.add(0, generateNewBlockMessage(bis), n, daspid);
      }

      System.out.println(
          ""
              + samplesWithinRegion
              + " samples out of "
              + b.getNumSamples()
              + " samples are within a node's region");

      System.out.println("" + totalSamples + " total samples distributed");
      if (samplesWithinRegion != b.getNumSamples()) {
        System.out.println(
            "Error: there are "
                + (b.getNumSamples() - samplesWithinRegion)
                + " samples that are not within a region of a peer ");
        // System.exit(1);
      }
    }
    ID_GENERATOR++;
    first = false;
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
