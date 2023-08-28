package peersim.gossipsub;

import java.math.BigInteger;

/** A Kademlia node, identified by its ID, IP address and port. */
public class GossipNode implements Comparable<GossipNode> {

  private BigInteger id;

  public GossipNode(BigInteger id) {
    this.id = id;
  }

  public BigInteger getId() {
    return this.id;
  }

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
    if (!(o instanceof GossipNode)) {
      return false;
    }
    // typecast o to KademliaNode so that we can compare data members
    GossipNode r = (GossipNode) o;

    return this.id.equals(r.id);
  }

  /**
   * Compares the node to another node to determine their order.
   *
   * @param n the node to compare to.
   * @return 0 if the nodes are equal, -1 if the node is less than the other node, 1 if the node is
   *     greater than the other node.
   */
  public int compareTo(GossipNode n) {
    if (this.id.compareTo(n.id) != 0) {
      return this.id.compareTo(n.id);
    }

    return 0;
  }
}
