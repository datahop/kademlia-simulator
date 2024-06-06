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
 * This control generates starts a DAS process every block time by sending a new block message to
 * all nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGeneratorSample implements Control {

  /** MSPastry Protocol ID to act */
  private final int kadpid;

  private static final String PAR_KADPROT = "kadprotocol";

  /** Mapping function for samples */
  final String PAR_MAP_FN = "mapping_fn";

  /** Number of sample copies stored per node */
  final String PAR_NUM_COPIES = "sample_copy_per_node";

  final String PAR_BLK_DIM_SIZE = "block_dim_size";

  final String PAR_NUM_SAMPLES = "num_samples";

  int mapfn;

  Block b;
  private long ID_GENERATOR = 0;
  long lastTime = 0;
  private boolean first = true;

  // ______________________________________________________________________________________________
  public TrafficGeneratorSample(String prefix) {
    kadpid = Configuration.getPid(prefix + "." + PAR_KADPROT);

    KademliaCommonConfigDas.MAPPING_FN = Configuration.getInt(prefix + "." + PAR_MAP_FN);
    KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER =
        Configuration.getInt(prefix + "." + PAR_NUM_COPIES);
    KademliaCommonConfigDas.BLOCK_DIM_SIZE =
        Configuration.getInt(
            prefix + "." + PAR_BLK_DIM_SIZE, KademliaCommonConfigDas.BLOCK_DIM_SIZE);
    KademliaCommonConfigDas.N_SAMPLES =
        Configuration.getInt(prefix + "." + PAR_NUM_SAMPLES, KademliaCommonConfigDas.N_SAMPLES);
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
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage(BigInteger id) {

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
    Block b = new Block(KademliaCommonConfigDas.BLOCK_DIM_SIZE, ID_GENERATOR);

    // In the first step only initialize doing 3 random lookups
    if (first) {
      for (int i = 0; i < Network.size(); i++) {
        Node start = Network.get(i);
        if (start.isUp()) {
          for (int j = 0; j < 3; j++) {
            // send message
            EDSimulator.add(0, generateFindNodeMessage(), start, kadpid);
          }
          EDSimulator.add(
              0,
              generateFindNodeMessage(start.getKademliaProtocol().getKademliaNode().getId()),
              start,
              kadpid);
        }
      }
      first = false;

      // After the first step start doing DAS sending a new block message to all nodes
    } else {

      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);

        if (n.isUp())
          EDSimulator.add(
              CommonState.r.nextLong(CommonState.getTime() - lastTime),
              generateFindNodeMessage(),
              n,
              kadpid);

        if (n.getDASProtocol() instanceof DASProtocolBuilder
            && n.getDASProtocol()
                .getBuilderAddress()
                .equals(n.getKademliaProtocol().getKademliaNode().getId())) {
          System.out.println(
              CommonState.getTime()
                  + " New block Builder "
                  + n.getKademliaProtocol().getKademliaNode().getId()
                  + " "
                  + i);
        }
        EDSimulator.add(1, generateNewBlockMessage(b), n, n.getDASProtocol().getDASProtocolID());
      }
      ID_GENERATOR++;
    }
    lastTime = CommonState.getTime();
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
