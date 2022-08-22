package peersim.kademlia;

import java.math.BigInteger;
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
public class TrafficGeneratorSingle implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_PROT = "protocol";

  private boolean first = true;
  private boolean second = false;

  private static Integer topicCounter = 0;

  /** MSPastry Protocol ID to act */
  private final int pid;

  // ______________________________________________________________________________________________
  public TrafficGeneratorSingle(String prefix) {
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage() {
    // existing active destination node
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
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {
    if (!first) {
      return false;
    }
    first = false;
    Node start;
    do {
      start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));

    // send message
    Message m = generateFindNodeMessage();
    // Message m = generateRegisterMessage();
    System.out.println(
        ">>>[" + CommonState.getTime() + "] Scheduling new MSG_FIND for " + (BigInteger) m.body);
    // sSystem.out.println(">>>[" + CommonState.getTime() + "] Scheduling new MSG_REGISTER for " +
    // ((Topic) m.body).getTopic());

    EDSimulator.add(0, m, start, pid);

    return false;
  }

  /*public boolean execute() {
  	Node start;
  	Message m = null;
  	do {
  		start = Network.get(CommonState.r.nextInt(Network.size()));
  	} while ((start == null) || (!start.isUp()));

  	if(first){
  		m = generateRegisterMessage();
  		System.out.println(">>>[" + CommonState.getTime() + "] Scheduling new Register for " + (Topic) m.body);
  		first = false;
  		second = true;
  	}else if (second) {
  		m = generateTopicLookupMessage();
  		System.out.println(">>>[" + CommonState.getTime() + "] Scheduling new lookup for " + (Topic) m.body);
  		first = false;
  		second = false;
  	}

  	if(m != null)
  		EDSimulator.add(0, m, start, pid);



  	return false;
  }*/
  /*
  * Used for debugging - submit a specific query
  public boolean execute() {
  	if(!first){
  		return false;
  	}
  	first = false;
  	System.out.println("[" + CommonState.getTime() + "] Scheduling new MSG_FIND");
  	Node n;
  	BigInteger id = new BigInteger("33748437612422773219378968224765130741693226654183676333640231977266549442093");
  	BigInteger dst = new BigInteger("10569885842503213978518658636404994036362621778380110644031029233192174040958");
  	for(int i = 0; i < Network.size(); i++){
  		n = Network.get(i);
  		KademliaProtocol kad = (KademliaProtocol) (n.getProtocol(pid));
  		BigInteger nodeId = kad.node.getId();
  		if(!nodeId.equals(new BigInteger("32723433973953629672970764487732486867255891608285193160907571170441265141575"))) continue;
  		int result = kad.routingTable.containsNode(dst);
  		if(result >= 0){
  			System.out.println(nodeId +  " contains node " + dst + " in its " + result + " bucket");
  			System.out.println(kad.routingTable.bucket(dst));
  		}
  	}

  	for(int i = 0; i < Network.size(); i++){
  		System.out.println("Checking node " + i);
  		n = Network.get(i);
  		BigInteger nodeId = ((KademliaProtocol) (n.getProtocol(pid))).node.getId();
  		//System.out.println("ID: " + id + " nodeId: " + nodeId);
  		if( nodeId.equals(id)){
  			//System.out.println("Found node " + id);
  			Message m = Message.makeFindNode(dst);
  			m.timestamp = CommonState.getTime();
  			EDSimulator.add(0, m, n, pid);
  			return false;
  		}
  	}

  	return false;
  }*/

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
