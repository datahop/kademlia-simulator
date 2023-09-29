package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.Node;

public class Neighbour implements Comparable<Neighbour> {

  BigInteger id;
  int last_seen;
  Node n;
  boolean isEvil;

  Neighbour(BigInteger id, Node n, boolean isEvil) {
    this.id = id;
    this.last_seen = 0;
    this.n = n;
    this.isEvil = isEvil;
  }

  public void updateLastSeen() {
    last_seen++;
  }

  public boolean expired() {
    if (last_seen >= KademliaCommonConfigDas.TTL) return true;
    else return false;
  }

  public Node getNode() {
    return n;
  }

  public boolean isEvil() {
    return isEvil;
  }

  @Override
  public int compareTo(Neighbour arg0) {
    if (this.last_seen < arg0.last_seen) return 1;
    else if (this.last_seen == arg0.last_seen) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object object)
  {
      boolean sameSame = false;

      if (object != null && object instanceof Neighbour)
      {
          sameSame = this.id.compareTo(((Neighbour) object).id)==0;
      }

      return sameSame;
  }
}
