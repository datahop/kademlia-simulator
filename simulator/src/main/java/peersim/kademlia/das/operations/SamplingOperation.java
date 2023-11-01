package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import peersim.core.CommonState;
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
  protected int aggressiveness;
  // protected HashSet<BigInteger> queried;

  protected int timesIncreased;
  protected BigInteger radiusValidator, radiusNonValidator;

  protected LinkedHashMap<BigInteger, Node> nodes;
  protected HashMap<BigInteger, FetchingSample> samples;

  protected List<BigInteger> askedNodes;
  protected List<BigInteger> pendingNodes;

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

    radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    samples = new HashMap<>();
    nodes = new LinkedHashMap<>();
    // this.available_requests = 0;
    aggressiveness = 0;
    askedNodes = new ArrayList<>();
    pendingNodes = new ArrayList<>();
    timesIncreased = 0;
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
    // this.available_requests = 0;
    aggressiveness = 0;
    radiusValidator =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, numValidators);
    radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    askedNodes = new ArrayList<>();
    timesIncreased = 0;
    pendingNodes = new ArrayList<>();
    // queried = new HashSet<>();
    // TODO Auto-generated constructor stub
  }

  public abstract boolean completed();

  public BigInteger[] getSamples() {
    List<BigInteger> result = new ArrayList<>();

    for (FetchingSample sample : samples.values()) {
      if (!sample.isDownloaded()) result.add(sample.getId());
    }

    return result.toArray(new BigInteger[0]);
  }

  public BigInteger getRadiusValidator() {
    return radiusValidator;
  }

  public BigInteger getRadiusNonValidator() {
    return radiusNonValidator;
  }

  protected abstract void createNodes();

  protected abstract void addExtraNodes();

  public BigInteger[] doSampling() {

    aggressiveness += KademliaCommonConfigDas.aggressiveness_step;
    // System.out.println("[" + srcNode + "]  nodes " + nodes.size());
    List<BigInteger> toRemove = new ArrayList<>();
    for (BigInteger n : nodes.keySet()) {
      List<FetchingSample> sToRemove = new ArrayList<>();
      for (FetchingSample s : nodes.get(n).getSamples()) {
        /*System.out.println(
        "["
            + srcNode
            + "]  nodes "
            + n
            + " "
            + nodes.get(n).isBeingAsked()
            + " "
            + nodes.get(n).getScore()
            + " "
            + aggressiveness
            + " "
            + s.isDownloaded()
            + " "
            + +s.beingFetchedFrom.size());*/
        if (s.isDownloaded()) sToRemove.add(s);
      }
      for (FetchingSample s : sToRemove) {
        nodes.get(n).removeFetchingSample(s);
      }
      if (nodes.get(n).getSamples().size() == 0) toRemove.add(n);
    }
    for (BigInteger id : toRemove) nodes.remove(id);

    if (nodes.isEmpty()) {
      createNodes();
      // System.out.println("[" + srcNode + "] Repopulating nodes " + nodes.size());
    }
    if (nodes.isEmpty()) {
      addExtraNodes();
      System.out.println(
          "["
              + CommonState.getTime()
              + "]["
              + srcNode
              + "] Adding extra nodes "
              + nodes.size()
              + " "
              + aggressiveness
              + " "
              + askedNodes.size());
    }
    for (Node n : nodes.values()) n.setAgressiveness(aggressiveness);
    List<BigInteger> result = new ArrayList<>();
    for (Node n : nodes.values()) {

      if (!n.isBeingAsked() && n.getScore() > 0) { // break;
        n.setBeingAsked(true);
        // this.available_requests++;
        pendingNodes.add(n.getId());
        for (FetchingSample s : n.getSamples()) {
          s.addFetchingNode(n);
        }
        result.add(n.getId());
      }
    }

    return result.toArray(new BigInteger[0]);
  }

  public Collection<Node> getNodes() {
    return nodes.values();
  }

  public abstract void elaborateResponse(Sample[] sam, BigInteger node);

  public abstract void elaborateResponse(Sample[] sam);

  public int samplesCount() {
    return samplesCount;
  }

  public int getPending() {
    return pendingNodes.size();
  }
}
