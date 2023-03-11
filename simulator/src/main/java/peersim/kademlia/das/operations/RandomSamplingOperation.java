package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.core.CommonState;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;
import peersim.kademlia.operations.FindOperation;

/**
 * This class represents a random sampling operation that collects samples from random nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class RandomSamplingOperation extends FindOperation {

  private SearchTable rou;
  private List<BigInteger> samples;
  /**
   * default constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public RandomSamplingOperation(
      BigInteger srcNode, BigInteger destNode, SearchTable rou, long timestamp) {
    super(srcNode, destNode, timestamp);
    this.rou = rou;
    samples = new ArrayList<>();
    for (BigInteger id : rou.getAllNeighbours()) closestSet.put(id, false);
  }

  public BigInteger getNeighbour() {

    BigInteger res = null;

    if (closestSet.size() > 0) {
      BigInteger[] results = (BigInteger[]) closestSet.keySet().toArray(new BigInteger[0]);
      res = results[CommonState.r.nextInt(results.length)];
    }

    if (res != null) {
      closestSet.remove(res);
      closestSet.put(res, true);
      // increaseUsed(res);
      this.available_requests--; // decrease available request
    }
    return res;
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      samples.add(s.getId());
    }
  }

  public BigInteger[] getSamples(Block b, BigInteger peerId) {

    return b.getSamplesByRadius(
        peerId, b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));
  }

  public List<BigInteger> getSamples() {
    return samples;
  }

  public boolean completed() {
    // System.out.println("Samples num " + samples.size());
    if (samples.size() >= KademliaCommonConfigDas.N_SAMPLES) return true;
    else return false;
  }
}
