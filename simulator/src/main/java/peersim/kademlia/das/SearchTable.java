package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class SearchTable {

  // private HashMap<BigInteger, List<BigInteger>> sampleMap;

  private Block currentBlock;

  private TreeSet<BigInteger> nodesIndexed; // , samplesIndexed;

  public SearchTable(Block currentblock) {

    this.currentBlock = currentblock;
    // this.sampleMap = new HashMap<>();
    this.nodesIndexed = new TreeSet<>();
    // this.samplesIndexed = new HashSet<>();
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

    /*for (BigInteger id : nodes) {
      BigInteger[] samples = getSamples(id);
      // System.out.println("Samples add " + samples.length);
      for (BigInteger sample : samples) {
        if (sampleMap.get(sample) == null) {
          List<BigInteger> ids = new ArrayList<>();
          ids.add(id);
          sampleMap.put(sample, ids);
          samplesIndexed.add(sample);
          nodesIndexed.add(id);
        } else {
          sampleMap.get(sample).add(id);
          nodesIndexed.add(id);
        }
      }
    }*/
    for (BigInteger id : nodes) nodesIndexed.add(id);
  }

  public TreeSet<BigInteger> nodesIndexed() {
    return nodesIndexed;
  }

  /*public HashSet<BigInteger> samplesIndexed() {
    return samplesIndexed;
  }*/

  public List<BigInteger> getNodesbySample(BigInteger sampleId) {

    BigInteger top =
        sampleId.add(
            currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));
    BigInteger bottom =
        sampleId.subtract(
            currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));

    Collection<BigInteger> subSet = nodesIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);

    // return sampleMap.get(sampleId);

  }

  public List<BigInteger> getNodesbySample(Set<BigInteger> samples) {

    List<BigInteger> result = new ArrayList<>();

    for (BigInteger sample : samples) {
      // if (sampleMap.get(sample) != null) result.addAll(sampleMap.get(sample));
      result.addAll(getNodesbySample(sample));
    }
    return result;
  }
}
