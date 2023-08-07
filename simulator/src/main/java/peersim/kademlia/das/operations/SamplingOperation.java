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

  protected BigInteger radiusValidator, radiusNonValidator;

  public SamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block block,
      boolean isValidator,
      int numValidators) {
    super(srcNode, destNode, timestamp);
    samples = new HashMap<BigInteger, Boolean>();
    completed = false;
    this.isValidator = isValidator;
    currentBlock = block;
    radiusValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    radiusNonValidator =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, numValidators);
  }

  public SamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block block,
      boolean isValidator,
      int numValidators,
      MissingNode callback) {
    super(srcNode, destNode, timestamp);
    samples = new HashMap<BigInteger, Boolean>();
    completed = false;
    this.isValidator = isValidator;
    this.callback = callback;
    currentBlock = block;

    radiusValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    radiusNonValidator =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, numValidators);
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

  public BigInteger getRadiusValidator() {
    return radiusValidator;
  }

  public BigInteger getRadiusNonValidator() {
    return radiusNonValidator;
  }

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
        List<BigInteger> validatorsBySample = searchTable.getNodesbySample(sample, radiusValidator);
        List<BigInteger> nonValidatorsBySample =
            searchTable.getNodesbySample(sample, radiusNonValidator);

        boolean found = false;
        if (validatorsBySample != null && validatorsBySample.size() > 0) {
          nodes.addAll(validatorsBySample);
          found = true;
        }
        if (nonValidatorsBySample != null && nonValidatorsBySample.size() > 0) {
          nodes.addAll(nonValidatorsBySample);
          found = true;
        }
        if (!found && callback != null) callback.missing(sample, this);
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

  public boolean increaseRadius(int multiplier) {
    radiusValidator = radiusValidator.multiply(BigInteger.valueOf(multiplier));
    if (Block.MAX_KEY.compareTo(radiusValidator) <= 0) {
      radiusValidator = Block.MAX_KEY;
      return false;
    }
    return true;
  }

  public abstract void elaborateResponse(Sample[] sam);

  public int samplesCount() {
    return samplesCount;
  }
}
