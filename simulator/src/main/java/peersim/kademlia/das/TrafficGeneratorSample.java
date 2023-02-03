package peersim.kademlia.das;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
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

  Block b;
  private boolean first = true, second = true;
  // ______________________________________________________________________________________________
  public TrafficGeneratorSample(String prefix) {
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
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
  private Message generateGetSampleMessage(Sample s) {

    Message m = Message.makeInitGetValue(s.getId());
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

    Node start;
    do {
      start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));

    if (first) {
      b = new Block(10);

      while (b.hasNext()) EDSimulator.add(0, generatePutSampleMessage(b.next()), start, pid);
      b.initIterator();
      first = false;
    } else if (second) {

      while (b.hasNext()) EDSimulator.add(0, generateGetSampleMessage(b.next()), start, pid);

      second = false;
    }
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
