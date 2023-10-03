package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.core.Node;

public class Neighbour implements Comparable<Neighbour> {

  protected BigInteger id;
  protected long last_seen;
  protected Node n;
  protected boolean isEvil;

  Neighbour(BigInteger id, Node n, boolean isEvil) {
    this.id = id;
    this.last_seen = CommonState.getTime();
    this.n = n;
    this.isEvil = isEvil;
  }

  public BigInteger getId() {
    return id;
  }

  public void updateLastSeen(long time) {
    last_seen = time;
  }

  public boolean expired() {
    if (CommonState.getTime() - last_seen >= KademliaCommonConfigDas.TTL) return true;
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
  public boolean equals(Object object) {
    boolean sameSame = false;

    if (object != null && object instanceof Neighbour) {
      sameSame = this.id.compareTo(((Neighbour) object).id) == 0;
    }

    return sameSame;
  }
}
