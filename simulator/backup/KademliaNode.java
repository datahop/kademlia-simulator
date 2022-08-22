package peersim.kademlia;

import java.math.BigInteger;
import java.util.List;
import peersim.core.Node;

public class KademliaNode implements Comparable<KademliaNode> {
  private BigInteger id;
  // attackerId is the ID used by Sybil nodes (when multiple nodes
  private BigInteger attackerID;
  private String addr;
  private int port;

  private Node n;

  // private Discv5ZipfTrafficGenerator client;

  boolean requested = false;

  public boolean is_evil = false;
  // all the topics the node registers for
  String myTopic = "";

  // private List<String> topicList;

  public KademliaNode(BigInteger id, String addr, int port) {
    this.id = id;
    this.attackerID = null;
    this.addr = addr;
    this.port = port;
  }

  public KademliaNode(BigInteger id, BigInteger attackerId, String addr, int port) {
    this.id = id;
    this.attackerID = attackerId;
    this.addr = addr;
    this.port = port;
  }

  public KademliaNode(BigInteger id) {
    this.id = id;
    this.addr = "127.0.0.1";
    this.port = 666;
    this.attackerID = null;
  }

  public KademliaNode(KademliaNode n) {
    this.id = n.id;
    this.addr = n.addr;
    this.port = n.port;
    this.is_evil = n.is_evil;
    this.attackerID = n.attackerID;
  }

  public BigInteger getId() {
    return this.id;
  }

  public BigInteger getAttackerId() {
    return this.attackerID;
  }

  public String getAddr() {
    return this.addr.toString();
  }

  public int getPort() {
    return this.port;
  }

  @Override
  public int hashCode() {
    return this.id.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    // If the object is compared with itself then return true
    if (o == this) {
      return true;
    }

    /* Check if o is an instance of KademliaNode or not
    "null instanceof [type]" also returns false */
    if (!(o instanceof KademliaNode)) {
      return false;
    }
    // typecast o to KademliaNode so that we can compare data members
    KademliaNode r = (KademliaNode) o;

    return this.id.equals(r.id);
  }

  public int compareTo(KademliaNode n) {
    if (this.id.compareTo(n.id) != 0) {
      return this.id.compareTo(n.id);
    }
    if (!this.addr.equals(n.addr)) {
      // return this.addr.compareTo(n.addr);
      return -1;
    }

    if (this.port == n.port) {
      return 0;
    }

    if (this.port < n.port) {
      return -1;
    }

    if (this.port > n.port) {
      return 1;
    }

    return 0;
  }

  public void setTopic(String t, Node n) {
    this.n = n;
    // we know allow only one topic per node
    // make sure it's the case
    assert (this.myTopic == "");
    myTopic = t;
  }

  public String getTopic() {
    return this.myTopic;
  }

  public int getTopicNum() {
    // topics are in a form t<topic_num>, e.g., t1, t5
    return Integer.valueOf(this.myTopic.substring(1));
  }

  public boolean hasTopic(String topic) {
    return this.myTopic.equals(topic);
  }

  public void setTopicList(List<String> t, Node n) {
    for (String topic : t) setTopic(topic, n);
  }
}
