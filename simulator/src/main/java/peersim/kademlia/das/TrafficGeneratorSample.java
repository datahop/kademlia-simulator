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
  private final int dasbuildpid;

  private final int dasvalpid;
  private final int dasnonvalpid;

  private static final String PAR_DASBUILDPROT = "dasbuildprotocol";
  private static final String PAR_DASVALPROT = "dasvalprotocol";
  private static final String PAR_DASNONVALPROT = "dasnonprotocol";

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

  // ______________________________________________________________________________________________
  public TrafficGeneratorSample(String prefix) {
    dasbuildpid = Configuration.getPid(prefix + "." + PAR_DASBUILDPROT);
    dasvalpid = Configuration.getPid(prefix + "." + PAR_DASVALPROT);
    dasnonvalpid = Configuration.getPid(prefix + "." + PAR_DASNONVALPROT);

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

    for (int i = 0; i < Network.size(); i++) {
      Node n = Network.get(i);
      if (n.isUp()) {
        // EDSimulator.add(0, generateNewBlockMessage(b), n, n.getDASProtocol().getDASProtocolID());
        // boolean successful = false;
        try {
          System.out.println("New block " + CommonState.getTime() + " " + b.getBlockId());
          EDSimulator.add(0, generateNewBlockMessage(b), n, dasbuildpid);
          // successful = true;
        } catch (Exception e) {
          System.out.println("Traffic error " + e);
        }
        /*if (!successful) {
          try {
            EDSimulator.add(0, generateNewBlockMessage(b), n, dasvalpid);
            successful = true;
          } catch (Exception e) {
          }
        }
        if (!successful) {
          try {
            EDSimulator.add(0, generateNewBlockMessage(b), n, dasnonvalpid);
          } catch (Exception e) {
          }
        }*/
      }
    }
    ID_GENERATOR++;
    lastTime = CommonState.getTime();
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
