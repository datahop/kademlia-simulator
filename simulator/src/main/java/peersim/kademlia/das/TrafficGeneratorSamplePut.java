package peersim.kademlia.das;

import java.math.BigInteger;
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
public class TrafficGeneratorSamplePut implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_PROT = "protocol";

  /** MSPastry Protocol ID to act */
  private final int pid;

  Block b;
  private boolean first = true, second = true;
  // ______________________________________________________________________________________________
  public TrafficGeneratorSamplePut(String prefix) {
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

    Node start;
    do {
      start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));

    if (first) {
      b = new Block(10);

      while (b.hasNext()) EDSimulator.add(0, generatePutSampleMessage(b.next()), start, pid);
      first = false;
    } else if (second) {
      b.initIterator();

      for (int i = 0; i < Network.size(); i++) {
        start = Network.get(i);
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
