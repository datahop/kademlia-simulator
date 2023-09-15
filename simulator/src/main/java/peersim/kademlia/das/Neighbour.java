package peersim.kademlia.das;

import java.math.BigInteger;

public class Neighbour implements Comparable<Neighbour> {

  BigInteger id;
  int last_seen;

  Neighbour(BigInteger id) {
    this.id = id;
    this.last_seen = 0;
  }

  public void updateLastSeen() {
    last_seen++;
  }

  public boolean expired() {
    if (last_seen >= KademliaCommonConfigDas.TTL) return true;
    else return false;
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
}
