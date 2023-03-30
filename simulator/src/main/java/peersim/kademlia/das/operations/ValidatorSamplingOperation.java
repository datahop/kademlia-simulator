package peersim.kademlia.das.operations;

import java.math.BigInteger;

import peersim.kademlia.operations.FindOperation;

public class ValidatorSamplingOperation extends FindOperation {
    
    /**
     * defaul constructor
     *
     * @param destNode Id of the node to find
     */
    public ValidatorSamplingOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
      super(srcNode, destNode, timestamp);
    }
  

}
