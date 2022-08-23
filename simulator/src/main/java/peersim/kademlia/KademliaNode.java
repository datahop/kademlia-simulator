package peersim.kademlia;

import java.math.BigInteger;

public class KademliaNode implements Comparable<KademliaNode> {
  private BigInteger id;
  // attackerId is the ID used by Sybil nodes (when multiple nodes
  private BigInteger attackerID;
  private String addr;
  private int port;
  private boolean is_evil;
  // all the topics the node registers for
  String myTopic = "";

  // private List<String> topicList;

  public KademliaNode(BigInteger id, String addr, int port) {
    this.id = id;
    this.attackerID = null;
    this.addr = addr;
    this.port = port;
    this.is_evil = false;
  }

  public KademliaNode(BigInteger id, BigInteger attackerId, String addr, int port) {
    this.id = id;
    this.attackerID = attackerId;
    this.addr = addr;
    this.port = port;
    this.is_evil = true;
  }

  public KademliaNode(BigInteger id) {
    this.id = id;
    this.addr = "127.0.0.1";
    this.port = 666;
    this.attackerID = null;
    this.is_evil = false;
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

  public boolean isEvil() {
    return is_evil;
  }

  public void setEvil(boolean evil) {
    this.is_evil = evil;
  }
}
