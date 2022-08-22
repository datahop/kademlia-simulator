package peersim.kademlia;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import peersim.core.Cleanable;
import peersim.core.Node;

public class Discv5Protocol extends KademliaProtocol implements Cleanable {

  public TopicTable topicTable;

  /** Table to keep track of topic registrations */
  protected HashSet<String> activeTopics;

  public Discv5Protocol(String prefix) {

    super(prefix);

    activeTopics = new HashSet<String>();

    // TODO Auto-generated constructor stub
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv5Protocol dolly = new Discv5Protocol(Discv5Protocol.prefix);
    return dolly;
  }

  /**
   * This procedure is called only once and allow to inizialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
   */
  protected void _init() {
    // execute once

    if (_ALREADY_INSTALLED) return;

    super._init();
  }

  public List<String> getRegisteringTopics() {

    return new ArrayList<String>(activeTopics);
  }

  /**
   * manage the peersim receiving of the events
   *
   * @param myNode Node
   * @param myPid int
   * @param event Object
   */
  /**
   * manage the peersim receiving of the events
   *
   * @param myNode Node
   * @param myPid int
   * @param event Object
   */
  public void processEvent(Node myNode, int myPid, Object event) {

    // this.discv5id = myPid;
    if (topicTable == null) return;
    super.processEvent(myNode, myPid, event);
  }

  /**
   * set the current NodeId
   *
   * @param tmp BigInteger
   */
  public void setNode(KademliaNode node) {
    super.setNode(node);
  }

  public void onKill() {
    // System.out.println("Node removed");
    topicTable = null;
  }
}
