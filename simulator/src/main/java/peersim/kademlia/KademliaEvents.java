package peersim.kademlia;

import java.math.BigInteger;
import peersim.kademlia.operations.Operation;

public interface KademliaEvents {

  public void nodesFound(Operation op, BigInteger[] neighbours);

  public void operationComplete(Operation op);
}
