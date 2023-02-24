package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
// import java.util.Random;
import peersim.core.CommonState;

public class SearchTable {

  private List<BigInteger> nodes;

  public SearchTable() {

    nodes = new ArrayList<>();
  }

  // add a neighbour to the correct k-bucket
  public void addNode(BigInteger[] neighbours) {
    nodes.addAll(Arrays.asList(neighbours));
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
}
