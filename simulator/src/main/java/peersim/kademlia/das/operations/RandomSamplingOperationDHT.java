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
    samples.clear();
    for (Sample rs : randomSamples) {
      samples.put(rs.getId(), false);
    }
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (samples.containsKey(s.getId())) {
        if (!samples.get(s.getId())) {
          samples.remove(s.getId());
          samples.put(s.getId(), true);
          samplesCount++;
        }
      }
    }
    // System.out.println("Samples received " + samples.size());
  }
}
