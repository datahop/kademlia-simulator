package peersim.kademlia;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

/**
 * This control generates random search traffic from nodes to random destination node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGeneratorRegionBased extends TrafficGenerator {

  // ______________________________________________________________________________________________
  public TrafficGeneratorRegionBased(String prefix) {
    super(prefix);
  }

  // ______________________________________________________________________________________________

  /**
   * generates a random region-based find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateRegionBasedFindNodeMessage() {
    // existing active destination node
    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    BigInteger id = urg.generate();
    int numHonest = 16;
    Message m = Message.makeInitRegionBasedFindNode(id, numHonest);
    m.timestamp = CommonState.getTime();

    return m;
  }

  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {

    Node start;
    do {
      start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));

    // send message
    EDSimulator.add(0, generateRegionBasedFindNodeMessage(), start, pid);

    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
