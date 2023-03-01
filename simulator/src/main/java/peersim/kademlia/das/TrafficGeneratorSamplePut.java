package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaNode;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.Message;

/**
 * This control generates random search traffic from nodes to random destination node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGeneratorSamplePut implements Control {

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
  public TrafficGeneratorSamplePut(String prefix) {
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
    KademliaCommonConfig.MAPPING_FN = Configuration.getInt(prefix + "." + PAR_MAP_FN);
    KademliaCommonConfig.NUM_SAMPLE_COPIES_PER_PEER =
        Configuration.getInt(prefix + "." + PAR_NUM_COPIES);
    KademliaCommonConfig.BLOCK_DIM_SIZE =
        Configuration.getInt(prefix + "." + PAR_BLK_DIM_SIZE, KademliaCommonConfig.BLOCK_DIM_SIZE);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a PUT message for t1 key and string message
   *
   * @return Message
   */
  private Message generatePutSampleMessage(Sample s) {

    Message m = Message.makeInitPutValue(s.getId(), s);
    m.timestamp = CommonState.getTime();
    System.out.println("Put message " + m.body + " " + m.value);
    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateGetSampleMessage(BigInteger sampleId) {

    Message m = Message.makeInitGetValue(sampleId);
    m.timestamp = CommonState.getTime();
    System.out.println("Get message " + m.body + " " + m.value);

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {

    /*
    Node start;
    do {
      start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));
    */
    /*
    if (first) {
      b = new Block(10);

      while (b.hasNext()) {
        EDSimulator.add(0, generatePutSampleMessage(b.next()), start, pid);
      }
      first = false;
    }*/
    if (first) {
      b = new Block(KademliaCommonConfig.BLOCK_DIM_SIZE);
      System.out.println("Number of samples in the block: " + b.getNumSamples());
      BigInteger radius = b.computeRegionRadius(KademliaCommonConfig.NUM_SAMPLE_COPIES_PER_PEER);
      int samplesWithinRegion = 0; // samples that are within at least one node's region
      while (b.hasNext()) {
        Sample s = b.next();
        boolean inRegion = false;
        for (int i = 0; i < Network.size(); i++) {
          Node n = Network.get(i);
          KademliaProtocol kadProt = ((KademliaProtocol) (n.getProtocol(pid)));
          KademliaNode kadNode = kadProt.getNode();
          if (n.isUp() && s.isInRegion(kadNode.getId(), radius)) {

            EDSimulator.add(0, generatePutSampleMessage(s), n, pid);
            if (inRegion == false) {
              samplesWithinRegion++;
              inRegion = true;
              System.out.println(
                  s.getId().toString(2)
                      + " is within the radius of "
                      + kadNode.getId().toString(2));
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
      if (samplesWithinRegion != b.getNumSamples()) {
        System.out.println(
            "Error: there are "
                + (b.getNumSamples() - samplesWithinRegion)
                + " samples that are not within a region of a peer ");
        System.exit(1);
      }
    } else if (second) {
      b.initIterator();

      for (int i = 0; i < Network.size(); i++) {
        Node start = Network.get(i);
        if (start.isUp()) {
          BigInteger[] samples = b.getNRandomSamplesIds(10);
          for (int j = 0; j < samples.length; j++) {
            EDSimulator.add(0, generateGetSampleMessage(samples[j]), start, pid);
          }
        }
      }

      second = false;
    }
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
