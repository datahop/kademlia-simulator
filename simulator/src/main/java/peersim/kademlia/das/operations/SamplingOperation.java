package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import peersim.core.CommonState;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;
import peersim.kademlia.operations.FindOperation;

public abstract class SamplingOperation extends FindOperation {

  protected HashMap<BigInteger, Boolean> samples;
  protected SearchTable searchTable;
  protected int samplesCount = 0;

  public SamplingOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, timestamp);
    samples = new HashMap<BigInteger, Boolean>();

    // TODO Auto-generated constructor stub
  }

  // public abstract void elaborateResponse(Sample[] sam);

  public abstract boolean completed();

  public abstract BigInteger[] getSamples(BigInteger peerId);

  // public abstract BigInteger[] startSampling();

  public abstract BigInteger[] doSampling();

  public BigInteger getNeighbour() {

    BigInteger res = null;
    List<BigInteger> nodes = new ArrayList<>();
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample)) {
        List<BigInteger> nodesBySample = searchTable.getNodesbySample(sample);
        if (nodesBySample != null) nodes.addAll(nodesBySample);
      }
    }
    // System.out.println("Get neighbour " + nodes.size());
    if (nodes.size() > 0) {
      // while (closestSet.get(res) != null)
      res = nodes.get(CommonState.r.nextInt(nodes.size()));
      while (closestSet.get(res) != null && nodes.size() > 0) {
        res = nodes.get(CommonState.r.nextInt(nodes.size()));
        nodes.remove(res);
      }
    }

    if (res != null) {
      // closestSet.remove(res);
      closestSet.put(res, true);
      // increaseUsed(res);
      this.available_requests--; // decrease available request
    }
    return res;
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      samples.remove(s.getId());
      samples.put(s.getId(), true);
    }
    System.out.println("Samples received " + samples.size());
  }

  public int samplesCount() {
    return samplesCount;
  }

  // public abstract void addNodes(List<BigInteger> nodes);
}
