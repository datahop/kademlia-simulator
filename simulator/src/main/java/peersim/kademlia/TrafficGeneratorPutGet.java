package peersim.kademlia;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
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
public class TrafficGeneratorPutGet implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_PROT = "protocol";

  /** MSPastry Protocol ID to act */
  private final int pid;

  private boolean first = true;
  // ______________________________________________________________________________________________
  public TrafficGeneratorPutGet(String prefix) {
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
  }

  // ______________________________________________________________________________________________
  /**
   * Generates a PUT message for t1 key and string message
   *
   * @return Message
   */
  private Message generatePutMessage() {

    // Existing active destination node
    MessageDigest digest;
    BigInteger id;
    String value = "hello";
    try {
      String topic = "t1";
      digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(topic.getBytes(StandardCharsets.UTF_8));
      id = new BigInteger(1, hash);
      Message m = Message.makeInitPutValue(id, value);
      m.timestamp = CommonState.getTime();
      // System.out.println("Put message " + m.body + " " + m.value);
      return m;
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }
  }

  // ______________________________________________________________________________________________
  /**
   * Generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateGetMessage() {

    MessageDigest digest;
    BigInteger id;
    try {
      String topic = "t1";
      digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(topic.getBytes(StandardCharsets.UTF_8));
      id = new BigInteger(1, hash);
      Message m = Message.makeInitGetValue(id);
      m.timestamp = CommonState.getTime();

      return m;
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }
  }

  // ______________________________________________________________________________________________
  /**
   * Every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {

    Node start;
    do {
      start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));

    if (first) {
      EDSimulator.add(0, generatePutMessage(), start, pid);
      first = false;
    } else {
      EDSimulator.add(0, generateGetMessage(), start, pid);
    }
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
