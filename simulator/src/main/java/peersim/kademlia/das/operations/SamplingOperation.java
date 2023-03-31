package peersim.kademlia.das.operations;

import java.math.BigInteger;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.Sample;
import peersim.kademlia.operations.FindOperation;

public abstract class SamplingOperation extends FindOperation {

  public SamplingOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, timestamp);
    // TODO Auto-generated constructor stub
  }

  public abstract void elaborateResponse(Sample[] sam);

  public abstract boolean completed();

  public abstract BigInteger[] getSamples(Block b, BigInteger peerId);

  public abstract BigInteger[] startSampling();

  public abstract BigInteger[] continueSampling();
}
