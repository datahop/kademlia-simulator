package peersim.kademlia.das;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.Message;

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
  private static final String PAR_PROT = "protocol";

  /** MSPastry Protocol ID to act */
  private final int pid;

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
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
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
  private Message generateNewBlockMessage(Block b) {

    Message m = Message.makeInitNewBlock(b);
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

    for (int i = 0; i < Network.size(); i++) {
      Node start = Network.get(i);
      if (start.isUp()) {
        EDSimulator.add(0, generateNewBlockMessage(b), start, pid);
      }
    }

    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
