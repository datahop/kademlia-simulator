package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import peersim.core.CommonState;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;
import peersim.kademlia.operations.FindOperation;

public abstract class SamplingOperation extends FindOperation {

  protected HashMap<BigInteger, Boolean> samples;
  protected SearchTable searchTable;
  protected int samplesCount = 0;
  protected boolean completed;
  protected boolean isValidator;
  protected MissingNode callback;

  public SamplingOperation(
      BigInteger srcNode, BigInteger destNode, long timestamp, boolean isValidator) {
    super(srcNode, destNode, timestamp);
    samples = new HashMap<BigInteger, Boolean>();
    completed = false;
    this.isValidator = isValidator;
  }

  public SamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      boolean isValidator,
      MissingNode callback) {
    super(srcNode, destNode, timestamp);
    samples = new HashMap<BigInteger, Boolean>();
    completed = false;
    this.isValidator = isValidator;
    this.callback = callback;
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
        else if (callback != null) callback.missing(sample, this);
      }
      //if (nodes.size() >= KademliaCommonConfig.ALPHA) break;
    }
    // System.out.println(srcNode + " Get neighbour " + nodes.size() + " " + samples.size());
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

  public abstract void elaborateResponse(Sample[] sam);

  public int samplesCount() {
    return samplesCount;
  }

  // public abstract void addNodes(List<BigInteger> nodes);
}
