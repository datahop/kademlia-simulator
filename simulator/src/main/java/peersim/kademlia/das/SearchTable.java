package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
// import java.util.Random;
import peersim.core.CommonState;

public class SearchTable {

  private List<BigInteger> nodes;

  Block b;
  Sample[] samples;

  public SearchTable() {

    nodes = new ArrayList<>();
  }

  // add a neighbour to the correct k-bucket
  public void addNode(BigInteger[] neighbours) {
    if (samples == null) nodes.addAll(Arrays.asList(neighbours));
    else {
      for (Sample s : samples) {
        BigInteger radius =
            b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
        for (BigInteger id : nodes) if (s.isInRegion(id, radius)) nodes.add(id);
      }
    }
  }

  public BigInteger getNode() {
    if (nodes.isEmpty()) return null;
    BigInteger node = nodes.get(CommonState.r.nextInt(nodes.size()));
    nodes.remove(node);
    return node;
  }

  // remove a neighbour from the correct k-bucket
  public void removeNeighbour(BigInteger node) {
    nodes.remove(node);
  }

  public int size() {
    return nodes.size();
  }

  public BigInteger[] getAllNeighbours() {
    return (BigInteger[]) nodes.toArray();
  }

  public void initSet(Block b) {

    Sample[] samples = b.getNRandomSamples(KademliaCommonConfigDas.N_SAMPLES);

    for (Sample s : samples) {
      BigInteger radius = b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
      for (BigInteger id : nodes) if (!s.isInRegion(id, radius)) nodes.remove(id);
    }
  }
}
