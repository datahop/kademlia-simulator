package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
// import peersim.kademlia.KademliaCommonConfig;
import peersim.core.Node;

public class SearchTable {

  private HashMap<BigInteger, Neighbour> neighbours;

  private TreeSet<BigInteger> nodesIndexed; // , samplesIndexed;

  private static TreeSet<BigInteger> validatorsIndexed = new TreeSet<>(); // , samplesIndexed;

  private TreeSet<BigInteger> nonValidatorsIndexed; // , samplesIndexed;

  private HashSet<BigInteger> blackList; // , samplesIndexed;

  private BigInteger builderAddress;

  public SearchTable(/*Block currentblock , BigInteger id*/ ) {

    // this.currentBlock = currentblock;
    // this.sampleMap = new HashMap<>();
    this.nodesIndexed = new TreeSet<>();
    this.nonValidatorsIndexed = new TreeSet<>();

    this.blackList = new HashSet<>();
    this.neighbours = new HashMap<>();
  }

  public void addNeighbour(BigInteger id, Node n) {
    if (builderAddress.compareTo(id) != 0) {
      if (neighbours.get(id) != null) {
        neighbours.remove(id);
        nodesIndexed.remove(id);
      }
      neighbours.put(id, new Neighbour(id, n, n.getDASProtocol().isEvil()));
      nodesIndexed.add(id);
    }
  }

  public void addNodes(BigInteger[] nodes) {

    for (BigInteger id : nodes) {
      if (!blackList.contains(id)
          && !validatorsIndexed.contains(id)
          && !builderAddress.equals(id)) {
        nonValidatorsIndexed.add(id);
      }
    }
  }

  public void seenNeighbour(BigInteger id, Node n) {
    if (builderAddress.compareTo(id) != 0) {
      if (neighbours.get(id) != null) {
        neighbours.remove(id);
        nodesIndexed.remove(id);
      }
      nodesIndexed.add(id);
      neighbours.put(id, new Neighbour(id, n, n.getDASProtocol().isEvil()));
    }
  }

  public void addValidatorNodes(BigInteger[] nodes) {
    for (BigInteger id : nodes) {
      if (!blackList.contains(id)) {
        validatorsIndexed.add(id);
      }
    }
  }

  public void setBuilderAddress(BigInteger builderAddress) {
    this.builderAddress = builderAddress;
  }

  public void removeNode(BigInteger node) {
    this.blackList.add(node);
    this.nodesIndexed.remove(node);
    this.nonValidatorsIndexed.remove(node);
  }

  public TreeSet<BigInteger> nodesIndexed() {
    return nodesIndexed;
  }

  public TreeSet<BigInteger> getValidatorsIndexed() {
    return validatorsIndexed;
  }

  public List<BigInteger> getNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;

    Collection<BigInteger> subSet = nodesIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);
  }

  public List<BigInteger> getValidatorNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;
    Collection<BigInteger> subSet = validatorsIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);
  }

  public List<BigInteger> getNonValidatorNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;

    Collection<BigInteger> subSet = nonValidatorsIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);
  }

  public List<BigInteger> getNodesbySample(Set<BigInteger> samples, BigInteger radius) {

    List<BigInteger> result = new ArrayList<>();

    for (BigInteger sample : samples) {
      result.addAll(getNodesbySample(sample, radius));
    }
    return result;
  }

  public BigInteger[] getNeighbours() {

    List<BigInteger> result = new ArrayList<>();
    List<Neighbour> neighs = new ArrayList<>();
    for (Neighbour n : neighbours.values()) {
      neighs.add(n);
    }
    Collections.sort(neighs);

    for (Neighbour neigh : neighs) {
      if (result.size() < KademliaCommonConfigDas.MAX_NODES_RETURNED) result.add(neigh.getId());
      else break;
    }
    return result.toArray(new BigInteger[0]);
  }

  public Neighbour[] getNeighbours(BigInteger id, BigInteger radius) {

    List<BigInteger> nodes = getNodesbySample(id, radius);
    List<Neighbour> neighs = new ArrayList<>();
    List<Neighbour> result = new ArrayList<>();
    for (BigInteger n : nodes) {
      neighs.add(neighbours.get(n));
    }
    // Collections.sort(neighs);
    Collections.shuffle(neighs);
    for (Neighbour neigh : neighs) {
      if (result.size() < KademliaCommonConfigDas.MAX_NODES_RETURNED) result.add(neigh);
      else break;
    }
    return result.toArray(new Neighbour[0]);
  }

  public int getAllNeighboursCount() {
    return neighbours.size();
  }

  public int getValidatorsNeighboursCount() {
    int count = 0;
    for (Neighbour neigh : neighbours.values()) {
      if (neigh.getNode().getDASProtocol().isValidator()) count++;
    }
    return count;
  }

  public int getNonValidatorsNeighboursCount() {
    int count = 0;
    for (Neighbour neigh : neighbours.values()) {
      if (!neigh.getNode().getDASProtocol().isValidator()) count++;
    }
    return count;
  }

  public int getAllAliveNeighboursCount() {
    int count = 0;
    for (Neighbour neigh : neighbours.values()) {
      if (neigh.getNode().isUp()) count++;
    }
    return count;
  }

  public int getMaliciousNeighboursCount() {
    int count = 0;
    for (Neighbour neigh : neighbours.values()) {
      if (neigh.isEvil()) count++;
    }
    return count;
  }

  public boolean isNeighbourKnown(BigInteger neighbour) {
    return neighbours.containsKey(neighbour);
  }

  public void refresh() {

    List<Neighbour> toRemove = new ArrayList<>();
    for (Neighbour neigh : neighbours.values()) {
      if (neigh.expired()) {
        toRemove.add(neigh);
        nodesIndexed.remove(neigh.getId());
      }
    }
    for (Neighbour n : toRemove) neighbours.remove(n.getId());
  }
}
