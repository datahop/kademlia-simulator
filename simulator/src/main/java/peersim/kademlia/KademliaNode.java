package peersim.kademlia;

import java.math.BigInteger;

/** A Kademlia node, identified by its ID, IP address and port. */
public class KademliaNode implements Comparable<KademliaNode> {
  private BigInteger id;
  /** attackerId is the ID used by Sybil nodes (when multiple nodes */
  private BigInteger attackerID;

  private String addr;
  private int port;
  private boolean is_evil;
  /** all the topics the node registers for */
  String myTopic = "";

  private boolean is_server;
  // private List<String> topicList;

  /**
   * Creates a new Kademlia node with the given ID, address and port.
   *
   * @param id the ID of the node
   * @param addr The IP address of the node
   * @param port The port of the node
   */
  public KademliaNode(BigInteger id, String addr, int port) {
    this.id = id;
    this.attackerID = null;
    this.addr = addr;
    this.port = port;
    this.is_evil = false;
    this.is_server = true;
  }

  /**
   * Creates a new Kademlia node with the given ID, attacker ID, address and port.
   *
   * @param id the ID of the node
   * @param attackerId the attackerID of the node
   * @param addr the IP address of the node
   * @param port the port of the node
   */
  public KademliaNode(BigInteger id, BigInteger attackerId, String addr, int port) {
    this.id = id;
    this.attackerID = attackerId;
    this.addr = addr;
    this.port = port;
    this.is_evil = true;
    this.is_server = true;
  }

  /**
   * Creates a new Kademlia node with the given ID and default address and port.
   *
   * @param id the ID of the node
   */
  public KademliaNode(BigInteger id) {
    this.id = id;
    this.addr = "127.0.0.1";
    this.port = 666;
    this.attackerID = null;
    this.is_evil = false;
    this.is_server = true;
  }

  /**
   * Creates a new Kademlia node as a copy of the given node.
   *
   * @param n the node to copy
   */
  public KademliaNode(KademliaNode n) {
    this.id = n.id;
    this.addr = n.addr;
    this.port = n.port;
    this.is_evil = n.is_evil;
    this.attackerID = n.attackerID;
    this.is_server = true;
  }

  /**
   * Returns the ID of the node.
   *
   * @return the ID of the node
   */
  public BigInteger getId() {
    return this.id;
  }

  /**
   * Returns the attacker ID of the node.
   *
   * @return the attacker ID of the node
   */
  public BigInteger getAttackerId() {
    return this.attackerID;
  }

  /**
   * Returns the IP address of the node.
   *
   * @return the IP address of the node
   */
  public String getAddr() {
    return this.addr.toString();
  }

  /**
   * Returns the port of the node.
   *
   * @return the port of the node
   */
  public int getPort() {
    return this.port;
  }

  /**
   * Computes the hash code of the node based on its ID.
   *
   * @return the hash code of the node.
   */
  @Override
  public int hashCode() {
    return this.id.hashCode();
  }

  /**
   * Compares the node to another object to check if they are equal.
   *
   * @param o the object to compare to.
   * @return true if the object is equal to the node, false otherwise.
   */
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

  /**
   * Compares the node to another node to determine their order.
   *
   * @param n the node to compare to.
   * @return 0 if the nodes are equal, -1 if the node is less than the other node, 1 if the node is
   *     greater than the other node.
   */
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

  /**
   * Checks whether the node is evil.
   *
   * @return true if the node is evil, false otherwise.
   */
  public boolean isEvil() {
    return is_evil;
  }

  /**
   * Sets whether the node is evil.
   *
   * @param evil true if the node is evil, false otherwise.
   */
  public void setEvil(boolean evil) {
    this.is_evil = evil;
  }

  public boolean isServer() {
    return is_server;
  }

  public void setServer(boolean server) {
    this.is_server = server;
  }
}
