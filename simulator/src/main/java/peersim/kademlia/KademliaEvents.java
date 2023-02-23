package peersim.kademlia;

import java.math.BigInteger;

public interface KademliaEvents {

  public void nodesFound(BigInteger[] neighbours);
}
