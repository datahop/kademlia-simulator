package peersim.gossipsub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.SearchTable;

/**
 * This control generates samples every 5 min that are stored in a single node (builder) and starts
 * random sampling from the rest of the nodes In parallel, random lookups are started to start
 * discovering nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGenerator implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_PROT = "protocol";

  /** Mapping function for samples */
  final String PAR_MAP_FN = "mapping_fn";

  /** Number of sample copies stored per node */
  final String PAR_NUM_COPIES = "sample_copy_per_node";

  final String PAR_BLK_DIM_SIZE = "block_dim_size";

  int mapfn;

  private boolean first = true, second = false;

  private long ID_GENERATOR = 0;

  // ______________________________________________________________________________________________
  public TrafficGenerator(String prefix) {

    GossipCommonConfig.BLOCK_DIM_SIZE =
        Configuration.getInt(prefix + "." + PAR_BLK_DIM_SIZE, GossipCommonConfig.BLOCK_DIM_SIZE);
  }

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

    Block b = new Block(GossipCommonConfig.BLOCK_DIM_SIZE, ID_GENERATOR);

    if (first) {
      SearchTable.createSampleMap(b);
      first = false;
    }
    // SearchTable.createSampleMap(b);
    for (int i = 0; i < Network.size(); i++) {
      Node n = Network.get(i);
      // b.initIterator();
      // we add 1 ms delay to be sure the builder starts before validators.
      EDSimulator.add(1, generateNewBlockMessage(b), n, n.getGossipProtocol().getDASProtocolID());
    }
    return false;
  }
  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
