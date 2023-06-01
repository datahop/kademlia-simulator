package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
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
  protected Block currentBlock;
  // protected HashSet<BigInteger> queried;

  protected BigInteger radius;

  public SamplingOperation(
      BigInteger srcNode, BigInteger destNode, long timestamp, Block block, boolean isValidator) {
    super(srcNode, destNode, timestamp);
    samples = new HashMap<BigInteger, Boolean>();
    completed = false;
    this.isValidator = isValidator;
    currentBlock = block;
    radius = currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
  }

  public SamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block block,
      boolean isValidator,
      MissingNode callback) {
    super(srcNode, destNode, timestamp);
    samples = new HashMap<BigInteger, Boolean>();
    completed = false;
    this.isValidator = isValidator;
    this.callback = callback;
    currentBlock = block;

    radius = currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

    // queried = new HashSet<>();
    // TODO Auto-generated constructor stub
  }
  // public abstract void elaborateResponse(Sample[] sam);

  public abstract boolean completed();

  // public abstract BigInteger[] getSamples(BigInteger peerId);

  public BigInteger[] getSamples() {
    List<BigInteger> result = new ArrayList<>();

    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample)) result.add(sample);
    }

    return result.toArray(new BigInteger[0]);
  }
  // public abstract BigInteger[] startSampling();

  public BigInteger[] doSampling() {

    List<BigInteger> nextNodes = new ArrayList<>();

    BigInteger nextNode;
    do {
      nextNode = getNeighbour();
      if (nextNode != null) nextNodes.add(nextNode);
    } while (nextNode != null);
    if (nextNodes.size() > 0) return nextNodes.toArray(new BigInteger[0]);
    return new BigInteger[0];
  }

  public BigInteger getNeighbour() {

    BigInteger res = null;
    List<BigInteger> nodes = new ArrayList<>();
    for (BigInteger sample : samples.keySet()) {

      if (!samples.get(sample)) {
        List<BigInteger> nodesBySample = searchTable.getNodesbySample(sample, radius);

        if (nodesBySample != null && nodesBySample.size() > 0) nodes.addAll(nodesBySample);
        else if (callback != null) callback.missing(sample, this);
      }
    }
    Collections.shuffle(nodes);

    for (BigInteger node : nodes) {
      if (closestSet.get(node) == null) {
        closestSet.put(node, true);
        this.available_requests--; // decrease available requets
        res = node;
        break;
      }
    }

    return res;
  }

  public void increaseRadius(int multiplier) {
    radius = radius.multiply(BigInteger.valueOf(multiplier));
  }

  public abstract void elaborateResponse(Sample[] sam);

  public int samplesCount() {
    return samplesCount;
  }
}
