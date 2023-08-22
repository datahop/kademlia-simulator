package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;
import peersim.kademlia.operations.FindOperation;

public abstract class SamplingOperation extends FindOperation {

  protected SearchTable searchTable;
  protected int samplesCount = 0;
  protected boolean completed;
  protected boolean isValidator;
  protected MissingNode callback;
  protected Block currentBlock;
  // protected HashSet<BigInteger> queried;

  protected BigInteger radiusValidator, radiusNonValidator;

  protected LinkedHashMap<BigInteger, Node> nodes;
  protected HashMap<BigInteger, FetchingSample> samples;

  public SamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block block,
      boolean isValidator,
      int numValidators) {
    super(srcNode, destNode, timestamp);
    completed = false;
    this.isValidator = isValidator;
    currentBlock = block;
    radiusValidator =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, numValidators);
    radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    samples = new HashMap<>();
    nodes = new LinkedHashMap<>();
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
    samples = new HashMap<>();
    nodes = new LinkedHashMap<>();
    completed = false;
    this.isValidator = isValidator;
    this.callback = callback;
    currentBlock = block;

    radiusValidator =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, numValidators);
    radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    // queried = new HashSet<>();
    // TODO Auto-generated constructor stub
  }
  // public abstract void elaborateResponse(Sample[] sam);

  public abstract boolean completed();

  // public abstract BigInteger[] getSamples(BigInteger peerId);

  public BigInteger[] getSamples() {
    List<BigInteger> result = new ArrayList<>();

    for (FetchingSample sample : samples.values()) {
      if (!sample.isDownloaded()) result.add(sample.getId());
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

  public abstract BigInteger[] doSampling();

  public BigInteger getNeighbour() {

    if (nodes.isEmpty()) {
      for (BigInteger sample : samples.keySet()) {
        if (!samples.get(sample).isDownloaded()) {
          List<BigInteger> validatorsBySample =
              searchTable.getValidatorNodesbySample(sample, radiusValidator);
          List<BigInteger> nonValidatorsBySample =
              searchTable.getNonValidatorNodesbySample(sample, radiusNonValidator);

          boolean found = false;
          if (validatorsBySample != null && validatorsBySample.size() > 0) {
            for (BigInteger id : validatorsBySample) {
              if (!nodes.containsKey(id)) nodes.put(id, new Node(id));
              nodes.get(id).addSample(sample);
            }
            found = true;
          }
          if (nonValidatorsBySample != null && nonValidatorsBySample.size() > 0) {
            for (BigInteger id : nonValidatorsBySample) {
              if (!nodes.containsKey(id)) nodes.put(id, new Node(id));

              nodes.get(id).addSample(sample);
            }
            found = true;
          }
          if (!found && callback != null) callback.missing(sample, this);
        }
      }
      Comparator<Entry<BigInteger, Node>> valueComparator =
          new Comparator<Entry<BigInteger, Node>>() {

            @Override
            public int compare(Entry<BigInteger, Node> e1, Entry<BigInteger, Node> e2) {
              Node v1 = e1.getValue();
              Node v2 = e2.getValue();
              return v1.compareTo(v2);
            }
          };
      List<Entry<BigInteger, Node>> nodeEntries =
          new ArrayList<Entry<BigInteger, Node>>(nodes.entrySet());
      Collections.sort(nodeEntries, valueComparator);

      nodes.clear();
      for (Entry<BigInteger, Node> entry : nodeEntries) {
        nodes.put(entry.getKey(), entry.getValue());
      }
    }

    for (Node n : nodes.values()) {
      if (!n.isBeingAsked() && n.getScore() > 0) return n.getId();
    }

    return null;
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
