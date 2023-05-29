package peersim.kademlia.das.operations;

import java.math.BigInteger;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;

public class RandomSamplingOperationDHT extends RandomSamplingOperation {

  public RandomSamplingOperationDHT(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block currentBlock,
      SearchTable searchTable,
      boolean isValidator,
      MissingNode callback) {
    super(srcNode, destNode, timestamp, currentBlock, searchTable, isValidator, callback);

    Sample[] randomSamples = currentBlock.getNRandomSamples(KademliaCommonConfigDas.N_SAMPLES);
    for (Sample rs : randomSamples) {
      samples.put(rs.getId(), false);
    }
  }
}
