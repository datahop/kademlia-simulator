package peersim.kademlia;

import java.math.BigInteger;
import java.util.List;
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
public class EthClient implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_PROT = "protocol";

  private boolean first = true;

  // private int topicCounter = 0;

  private int connections = 0;

  private static final int MAXCONNECTIONS = 50;

  UniformRandomGenerator urg;
  /** MSPastry Protocol ID to act */
  private final int pid;

  private List<BigInteger> buffer;
  // ______________________________________________________________________________________________
  public EthClient(String prefix) {
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
    urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage() {
    // existing active destination node
    /*Node n = Network.get(CommonState.r.nextInt(Network.size()));
    while (!n.isUp()) {
    	n = Network.get(CommonState.r.nextInt(Network.size()));
    }
    BigInteger dst = ((KademliaProtocol) (n.getProtocol(pid))).node.getId();*/

    // BigInteger dst=urg.generate();
    Node n = Network.get(CommonState.r.nextInt(Network.size()));
    while (!n.isUp()) {
      n = Network.get(CommonState.r.nextInt(Network.size()));
    }
    BigInteger dst = ((KademliaProtocol) (n.getProtocol(pid))).node.getId();

    Message m = Message.makeInitFindNode(dst);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a register message, by selecting randomly the destination.
   *
   * @return Message
   */
  /*private Message generateRegistersMessage() {
  	Topic t = new Topic("t" + Integer.toString(this.topicCounter++));
  	Message m = Message.makeRegister(t.getTopic());
  	m.timestamp = CommonState.getTime();


  	// existing active destination node
  	Node n = Network.get(CommonState.r.nextInt(Network.size()));
  	while (!n.isUp()) {
  		n = Network.get(CommonState.r.nextInt(Network.size()));
  	}
  	m.body = ((KademliaProtocol) (n.getProtocol(pid))).node.getId();

  	return m;
  }*/

  public void setNodeListResult(List<BigInteger> result) {
    // System.out.println("Buffer set at "+result.size());
    buffer = result;
  }

  public void emptyBufferCallback(Node n) {
    // System.out.println("Emptybuffer:" +((KademliaProtocol)n.getProtocol(pid)).getNode().getId());
    EDSimulator.add(0, generateFindNodeMessage(), n, pid);
  }
  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {
    // first = false;
    if (first) {
      // System.out.println("Execute first called at"+CommonState.getTime()+" "+Network.size());
      for (int i = 0; i < Network.size(); i++) {
        KademliaProtocol kad = (KademliaProtocol) Network.get(i).getProtocol(pid);
        // kad.setClient(this);
        // kad.getNode().setCallBack(this,Network.get(i));
        // System.out.println("Execute first called atc"+CommonState.getTime()+" "+Network.size());
        if (Network.get(i).isUp()) {
          // for(int j=0;j<3;j++)
          // {
          Message msg = generateFindNodeMessage();
          // System.out.println("Message from"+kad.getNode().getId()+" to "+(BigInteger)msg.body);
          EDSimulator.add(0, msg, Network.get(i), pid);
          // }
        }
      }
      first = false;
      return false;
    }
    // first = false;
    // System.out.println("Execute called at"+CommonState.getTime());
    /*Node start;
    do {
    	start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));

    // send message
    EDSimulator.add(0, generateFindNodeMessage(), start, pid);*/

    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
