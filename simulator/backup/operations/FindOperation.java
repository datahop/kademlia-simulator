package peersim.kademlia.operations;

import java.math.BigInteger;
import peersim.kademlia.Message;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the
 * closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class FindOperation extends Operation {

  private String topic;
  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public FindOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, Message.MSG_FIND, timestamp);
    // topics = new ArrayList<String>();
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }
  /*public void setDiscovered(int disc) {
  	this.discovered = disc;
  }*/

  public int getDiscovered() {

    int discovered = 0;
    // FIXME
    // we should keep discovered list in this class already
    /*for(BigInteger nodeId : this.getNeighboursList()) {
    	if (Util.nodeIdtoNode(nodeId).getKademliaProtocol().getNode().getTopicList().contains(topic))discovered++;

    }*/
    return discovered;
  }
}
