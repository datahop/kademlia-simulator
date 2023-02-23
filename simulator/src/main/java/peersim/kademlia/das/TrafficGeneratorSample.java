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
 * This control generates random search traffic from nodes to random destination node.
 *
 * @author Daniele Furlan, Maurizio Bonani
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
  private boolean first = true, second = true;
  // ______________________________________________________________________________________________
  public TrafficGeneratorSample(String prefix) {
    kadpid = Configuration.getPid(prefix + "." + PAR_KADPROT);
    daspid = Configuration.getPid(prefix + "." + PAR_DASPROT);

    KademliaCommonConfig.MAPPING_FN = Configuration.getInt(prefix + "." + PAR_MAP_FN);
    KademliaCommonConfig.NUM_SAMPLE_COPIES_PER_PEER =
        Configuration.getInt(prefix + "." + PAR_NUM_COPIES);
    KademliaCommonConfig.BLOCK_DIM_SIZE =
        Configuration.getInt(prefix + "." + PAR_BLK_DIM_SIZE, KademliaCommonConfig.BLOCK_DIM_SIZE);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateNewBlockMessage(Sample s) {

    Message m = Message.makeInitNewBlock(s);
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

    Block b = new Block(KademliaCommonConfig.BLOCK_DIM_SIZE);
    BigInteger radius = b.computeRegionRadius(KademliaCommonConfig.NUM_SAMPLE_COPIES_PER_PEER);
    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int totalSamples = 0;
    while (b.hasNext()) {
      Sample s = b.next();
      boolean inRegion = false;

      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        DASProtocol dasProt = ((DASProtocol) (n.getProtocol(daspid)));

        if (dasProt.isBuilder()) EDSimulator.add(0, generateNewBlockMessage(s), n, daspid);
        else if (n.isUp() && s.isInRegion(dasProt.getKademliaId(), radius)) {
          totalSamples++;
          EDSimulator.add(0, generateNewSampleMessage(s), n, daspid);
          if (inRegion == false) {
            samplesWithinRegion++;
            inRegion = true;
          }
        }
      }
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

    if (first) {
      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        if (n.isUp()) {
          for (int j = 0; j < 3; j++) {
            int time = CommonState.r.nextInt(300000);
            Node start = Network.get(i);
            Message lookup = generateFindNodeMessage();
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
          EDSimulator.add(time, lookup, start, kadpid);
        }
      }
    }
    first = false;
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
