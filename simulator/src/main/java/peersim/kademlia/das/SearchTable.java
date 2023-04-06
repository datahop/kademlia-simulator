package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import peersim.core.CommonState;

public class SearchTable {

  private HashMap<BigInteger, List<BigInteger>> sampleMap;

  private Block currentBlock;

  private HashSet<BigInteger> nodesIndexed, samplesIndexed;

  public SearchTable(Block currentblock) {

    this.currentBlock = currentblock;
    this.sampleMap = new HashMap<>();
    this.nodesIndexed = new HashSet<>();
    this.samplesIndexed = new HashSet<>();
  }

  public void setBlock(Block currentBlock) {
    this.currentBlock = currentBlock;
  }

  public BigInteger[] getSamples(Block b, BigInteger peerId) {

    return b.getSamplesByRadius(
        peerId, b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));
  }

  public void addNodes(BigInteger[] nodes) {

    for (BigInteger id : nodes) {
      BigInteger[] samples = getSamples(currentBlock, id);
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
    }
  }

  public HashSet<BigInteger> nodesIndexed() {
    return nodesIndexed;
  }

  public HashSet<BigInteger> samplesIndexed() {
    return samplesIndexed;
  }

  public BigInteger getRandomSample() {

    List<BigInteger> valuesList = new ArrayList<BigInteger>(sampleMap.keySet());
    return valuesList.get(CommonState.r.nextInt(valuesList.size()));
  }

  public List<BigInteger> getNodesbySample(BigInteger id) {

    return sampleMap.get(id);
  }
}
