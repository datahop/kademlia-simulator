package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import peersim.kademlia.Util;

public class SearchTable {

  // private HashMap<BigInteger, List<BigInteger>> sampleMap;

  private Block currentBlock;

  private TreeSet<BigInteger> nodesIndexed; // , samplesIndexed;

  private HashSet<BigInteger> blackList; // , samplesIndexed;

  public SearchTable(Block currentblock) {

    this.currentBlock = currentblock;
    // this.sampleMap = new HashMap<>();
    this.nodesIndexed = new TreeSet<>();
    this.blackList = new HashSet<>();
  }

  public void setBlock(Block currentBlock) {
    this.currentBlock = currentBlock;
  }

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
      if (!blackList.contains(id)) nodesIndexed.add(id);
    }
  }

  public void removeNode(BigInteger node) {
    this.blackList.add(node);
    this.nodesIndexed.remove(node);
  }

  public TreeSet<BigInteger> nodesIndexed() {
    return nodesIndexed;
  }

  /*public HashSet<BigInteger> samplesIndexed() {
    return samplesIndexed;
  }*/

  //Return nodes that should have the indicate sample
  public List<BigInteger> getNodesForSample(BigInteger sampleId, BigInteger radius) {
    ArrayList<BigInteger> result = new ArrayList<BigInteger>();
    // TODO we should make it more efficient (now it's O(n))
    for (BigInteger nodeId : nodesIndexed) {
      // if radius is larger than the distance between the node and the sample ID, then add the node
      // to the result
      if (radius.compareTo(Util.xorDistance(nodeId, sampleId)) == 1) result.add(nodeId);
    }
    return result;
  }

  public List<BigInteger> getNodesForSamples(BigInteger[] samples, BigInteger radius) {

    List<BigInteger> result = new ArrayList<>();

    for (BigInteger sample : samples) {
      // if (sampleMap.get(sample) != null) result.addAll(sampleMap.get(sample));
      result.addAll(getNodesForSample(sample, radius));
    }
    return result;
  }
}
