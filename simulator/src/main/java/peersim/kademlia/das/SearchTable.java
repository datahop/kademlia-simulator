package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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

  public BigInteger[] getSamples(BigInteger peerId) {

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
  }

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

  public List<BigInteger> getNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.valueOf(0);

    BigInteger maxValue = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(maxValue) == 1) top = maxValue;

    Collection<BigInteger> subSet = nodesIndexed.subSet(bottom, true, top, true);
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
}
