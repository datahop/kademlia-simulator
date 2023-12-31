package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

  protected List<BigInteger> askNodes;

  protected boolean extraNodesActivated;

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
    this.available_requests = 0;
    aggressiveness = 0;
    askNodes = new ArrayList<>();
    timesIncreased = 0;
    extraNodesActivated = false;
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
    this.available_requests = 0;
    aggressiveness = 0;
    radiusValidator =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, numValidators);
    radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    askNodes = new ArrayList<>();
    timesIncreased = 0;
    extraNodesActivated = false;

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

  abstract protected void createNodes();

  public boolean addExtraNodes() {
    if (!extraNodesActivated) {
      for (BigInteger sample : samples.keySet()) {
        if (!samples.get(sample).isDownloaded()) {

          List<BigInteger> nodesBySample = new ArrayList<>();
          // searchTable.getNodesbySample(samples.get(sample).getId(), radiusValidator);
          for (Sample s : currentBlock.getSamplesByRow(samples.get(sample).getRow())) {
            nodesBySample.addAll(searchTable.getValidatorNodesbySample(s.getId(), radiusValidator));
          }
          for (Sample s : currentBlock.getSamplesByColumn(samples.get(sample).getColumn())) {
            nodesBySample.addAll(
                searchTable.getValidatorNodesbySample(s.getIdByRow(), radiusValidator));
          }
          Collections.shuffle(nodesBySample);
          int nodesAssigned = 0;
          if (nodesBySample != null && nodesBySample.size() > 0) {
            for (BigInteger id : nodesBySample) {
              if (!nodes.containsKey(id)) {
                nodes.put(id, new Node(id));
                nodes.get(id).addSample(samples.get(sample));
                nodesAssigned++;
                if (nodesAssigned >= KademliaCommonConfigDas.EXTRA_NODES_MAX) break;
              } else {
                nodes.get(id).addSample(samples.get(sample));
              }
            }
          }
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public BigInteger[] doSampling() {

    aggressiveness += KademliaCommonConfigDas.aggressiveness_step;
    for (Node n : nodes.values()) n.setAgressiveness(aggressiveness);
    List<BigInteger> result = new ArrayList<>();
    for (Node n : nodes.values()) {
      /*System.out.println(
          this.srcNode + "] Querying node " + n.getId() + " " + +n.getScore() + " " + this.getId());
      for (FetchingSample fs : n.getSamples())
        System.out.println(
            this.srcNode + "] Sample " + fs.beingFetchedFrom.size() + " " + fs.isDownloaded());*/

      if (!n.isBeingAsked() && n.getScore() > 0) { // break;
        n.setBeingAsked(true);
        this.available_requests++;
        for (FetchingSample s : n.getSamples()) {
          s.addFetchingNode(n);
        }
        result.add(n.getId());
      }
    }

    return result.toArray(new BigInteger[0]);
  }

  public abstract void elaborateResponse(Sample[] sam, BigInteger node);

  public abstract void elaborateResponse(Sample[] sam);

  public int samplesCount() {
    return samplesCount;
  }
}
