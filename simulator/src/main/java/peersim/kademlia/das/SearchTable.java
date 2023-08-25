package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
// import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.RoutingTable;

public class SearchTable {

  // private HashMap<BigInteger, List<BigInteger>> sampleMap;

  // private Block currentBlock;

  private TreeSet<BigInteger> nodesIndexed; // , samplesIndexed;

  private TreeSet<BigInteger> validatorsIndexed; // , samplesIndexed;

  private TreeSet<BigInteger> nonValidatorsIndexed; // , samplesIndexed;

  private HashSet<BigInteger> blackList; // , samplesIndexed;

  private RoutingTable routingTable;

  private BigInteger builderAddress;

  public SearchTable(/*Block currentblock , BigInteger id*/ ) {

    // this.currentBlock = currentblock;
    // this.sampleMap = new HashMap<>();
    this.nodesIndexed = new TreeSet<>();
    this.validatorsIndexed = new TreeSet<>();
    this.nonValidatorsIndexed = new TreeSet<>();

    this.blackList = new HashSet<>();

    // routingTable = new RoutingTable(KademliaCommonConfig.K, KademliaCommonConfig.BITS, 0);
    // routingTable.setNodeId(id);
  }

  /*public void setBlock(Block currentBlock) {
    this.currentBlock = currentBlock;
  }*/

  /*public BigInteger[] getSamples(BigInteger peerId) {

    List<BigInteger> result = new ArrayList<>();
    Collections.addAll(
        result,
        currentBlock.getSamplesByRadiusByColumn(
            peerId,
            currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));
    Collections.addAll(
        result,
        currentBlock.getSamplesByRadiusByRow(
            peerId,
            currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));

    return result.toArray(new BigInteger[0]);
  }*/

  public void addNodes(BigInteger[] nodes) {

    for (BigInteger id : nodes) {
      if (!blackList.contains(id)
          && !validatorsIndexed.contains(id)
          && !builderAddress.equals(id)) {
        nonValidatorsIndexed.add(id);
      }
    }
  }

  /*public void addNonValidatorNodes(BigInteger[] nodes) {

    for (BigInteger id : nodes) {
      if (!blackList.contains(id) && !validatorsIndexed.contains(id)) {
        nodesIndexed.add(id);
      }
    }
  }*/

  public void addValidatorNodes(BigInteger[] nodes) {
    for (BigInteger id : nodes) {
      if (!blackList.contains(id)) {
        validatorsIndexed.add(id);
      }
      // routingTable.addNeighbour(id);
    }
  }

  public void setBuilderAddress(BigInteger builderAddress) {
    this.builderAddress = builderAddress;
  }

  public void removeNode(BigInteger node) {
    // this.blackList.add(node);
    this.nodesIndexed.remove(node);
    this.validatorsIndexed.remove(node);
    this.nonValidatorsIndexed.remove(node);
  }

  public TreeSet<BigInteger> nodesIndexed() {
    return nonValidatorsIndexed;
  }

  public TreeSet<BigInteger> getValidatorsIndexed() {
    return validatorsIndexed;
  }

  /*public HashSet<BigInteger> samplesIndexed() {
    return samplesIndexed;
  }*/

  public List<BigInteger> getNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;

    Collection<BigInteger> subSet = nodesIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);

    // return sampleMap.get(sampleId);

  }

  public List<BigInteger> getValidatorNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;
    Collection<BigInteger> subSet = validatorsIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);

    // return sampleMap.get(sampleId);

  }

  public List<BigInteger> getNonValidatorNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;

    Collection<BigInteger> subSet = nonValidatorsIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);

    // return sampleMap.get(sampleId);

  }

  public List<BigInteger> getNodesbySample(Set<BigInteger> samples, BigInteger radius) {

    List<BigInteger> result = new ArrayList<>();

    for (BigInteger sample : samples) {
      // if (sampleMap.get(sample) != null) result.addAll(sampleMap.get(sample));
      result.addAll(getNodesbySample(sample, radius));
    }
    return result;
  }

  /*public BigInteger[] findKClosestValidators(BigInteger sampleId) {

    return routingTable.getNeighbours(sampleId, sampleId);
  }*/
}
