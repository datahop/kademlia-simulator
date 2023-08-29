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
  protected int aggressiveness;
  // protected HashSet<BigInteger> queried;

  protected int timesIncreased;
  protected BigInteger radiusNonValidator;

  protected LinkedHashMap<BigInteger, Node> nodes;
  protected HashMap<BigInteger, FetchingSample> samples;

  protected List<BigInteger> askNodes;

  public SamplingOperation(
      BigInteger srcNode, BigInteger destNode, long timestamp, Block block, boolean isValidator) {
    super(srcNode, destNode, timestamp);
    completed = false;
    this.isValidator = isValidator;
    currentBlock = block;

    radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    samples = new HashMap<>();
    nodes = new LinkedHashMap<>();
    this.available_requests = 0;
    aggressiveness = 0;
    askNodes = new ArrayList<>();
    timesIncreased = 0;
  }

  public SamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block block,
      boolean isValidator,
      // int numValidators,
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
    // radiusValidator =
    //    currentBlock.computeRegionRadius(
    //        KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, numValidators);
    radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
    askNodes = new ArrayList<>();
    timesIncreased = 0;
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

  /*public BigInteger getRadiusValidator() {
    return radiusValidator;
  }*/

  public BigInteger getRadiusNonValidator() {
    return radiusNonValidator;
  }

  protected void createNodes() {
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample).isDownloaded()) {

        List<BigInteger> validatorsBySample = SearchTable.getNodesBySample(sample);

        boolean found = false;
        if (validatorsBySample != null && validatorsBySample.size() > 0) {
          for (BigInteger id : validatorsBySample) {
            if (!nodes.containsKey(id)) {
              nodes.put(id, new Node(id));
              nodes.get(id).addSample(samples.get(sample));
            } else {
              nodes.get(id).addSample(samples.get(sample));
            }
          }
          found = true;
        }

        if (searchTable != null) {
          List<BigInteger> nonValidatorsBySample =
              searchTable.getNonValidatorNodesbySample(sample, radiusNonValidator);
          if (nonValidatorsBySample != null && nonValidatorsBySample.size() > 0) {
            for (BigInteger id : nonValidatorsBySample) {
              if (!nodes.containsKey(id)) {
                nodes.put(id, new Node(id));
                nodes.get(id).addSample(samples.get(sample));
              } else {
                nodes.get(id).addSample(samples.get(sample));
              }
            }
            found = true;
          }
          // System.out.println(
          //     "Non-validator node map " + nonValidatorsBySample.size() + " " + sample);
        }

        if (!found && callback != null) callback.missing(sample, this);
      }
    }
  }

  public BigInteger[] doSampling() {

    aggressiveness += KademliaCommonConfigDas.aggressiveness_step;
    for (Node n : nodes.values()) n.setAgressiveness(aggressiveness);
    // System.out.println(
    //    this.srcNode + "] Do sampling " + aggressiveness + " " + this.getId() + " " +
    // nodes.size());
    // orderNodes();
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

  public boolean increaseRadius(int multiplier) {
    /* if (timesIncreased == KademliaCommonConfigDas.multiplyRadiusLimit) return false;
    radiusValidator = radiusValidator.multiply(BigInteger.valueOf(multiplier));
    if (Block.MAX_KEY.compareTo(radiusValidator) <= 0) {
      radiusValidator = Block.MAX_KEY;
      return false;
    }*/
    createNodes();
    return true;
  }

  private void orderNodes() {
    for (Node n : nodes.values()) n.setAgressiveness(aggressiveness);

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

    /*for (Node n : nodes.values()) {
      System.out.println(this.srcNode + "] Node created " + n.getId() + " " + n.getScore());
    }*/
  }

  public abstract void elaborateResponse(Sample[] sam, BigInteger node);

  public abstract void elaborateResponse(Sample[] sam);

  public int samplesCount() {
    return samplesCount;
  }
}
