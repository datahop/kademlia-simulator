package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.kademlia.operations.Operation;

public interface MissingNode {

  public void missing(BigInteger sample, Operation op);
}
