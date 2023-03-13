package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
// import java.util.Random;
import peersim.core.CommonState;

/** Structure used to store discovered nodes in the network and feed the random sampling process */
public class SearchTable {

  /** List of ids discovered */
  private List<BigInteger> nodes;

  public SearchTable() {

    nodes = new ArrayList<>();
  }

  // add a neighbour to the correct k-bucket
  public void addNode(BigInteger[] neighbours) {
    // if (samples == null)
    nodes.addAll(Arrays.asList(neighbours));
  }

  /** Get any randomn node from the list */
  public BigInteger getNode() {
    if (nodes.isEmpty()) return null;
    BigInteger node = nodes.get(CommonState.r.nextInt(nodes.size()));
    // nodes.remove(node);
    return node;
  }

  // remove the node from the list
  public void removeNeighbour(BigInteger node) {
    nodes.remove(node);
  }

  public int size() {
    return nodes.size();
  }

  /** Returns all nodes in the list */
  public BigInteger[] getAllNeighbours() {
    return (BigInteger[]) nodes.toArray(new BigInteger[0]);
  }
}
