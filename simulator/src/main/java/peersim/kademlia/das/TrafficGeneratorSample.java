package peersim.kademlia.das;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.Message;

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
  private boolean first = true, second = false;

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
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {
    Block b = new Block(KademliaCommonConfigDas.BLOCK_DIM_SIZE, ID_GENERATOR);
    System.out.println(CommonState.getTime() + " next");
    /*if (first) {
      for (int i = 0; i < Network.size(); i++) {
        Node start = Network.get(i);
        if (start.isUp()) {
          for (int j = 0; j < 3; j++) {
            // send message
            EDSimulator.add(
                CommonState.r.nextInt(12000), Util.generateFindNodeMessage(), start, kadpid);
          }
          EDSimulator.add(
              0,
              Util.generateFindNodeMessage(start.getKademliaProtocol().getKademliaNode().getId()),
              start,
              kadpid);
        }
      }
      first = false;
      second = true;
    } else if (second) {*/

    // SearchTable.createSampleMap(b);
    for (int i = 0; i < Network.size(); i++) {
      Node n = Network.get(i);
      // b.initIterator();
      // we add 1 ms delay to be sure the builder starts before validators.
      /*EDSimulator.add(
      CommonState.r.nextLong(CommonState.getTime() - lastTime),
      Util.generateFindNodeMessage(),
      n,
      kadpid);*/

      if (n.isUp()) {
        EDSimulator.add(0, generateNewBlockMessage(b), n, n.getDASProtocol().getDASProtocolID());
        // EDSimulator.add(10, generateNewBlockMessage(b), n,
        // n.getDASProtocol().getDASProtocolID());
      }
    }
    ID_GENERATOR++;
    second = false;
    // }
    lastTime = CommonState.getTime();
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
