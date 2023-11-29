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
import peersim.core.Node;

public class SearchTable {

  private HashMap<BigInteger, Neighbour> neighbours;

  private TreeSet<BigInteger> nodesIndexed; // , samplesIndexed;

  private static TreeSet<BigInteger> validatorsIndexed = new TreeSet<>(); // , samplesIndexed;

  private TreeSet<BigInteger> nonValidatorsIndexed; // , samplesIndexed;

  private HashSet<BigInteger> blackList; // , samplesIndexed;

  private BigInteger builderAddress;

  private List<Node> evilIds;

  public SearchTable() {

    this.nodesIndexed = new TreeSet<>();
    this.nonValidatorsIndexed = new TreeSet<>();

    this.blackList = new HashSet<>();
    this.neighbours = new HashMap<>();
  }

  public void addNeighbour(Neighbour neigh) {
    if (neigh.getId().compareTo(builderAddress) != 0) {
      if (neighbours.get(neigh.getId()) == null) {
        neighbours.put(neigh.getId(), neigh);
        nodesIndexed.add(neigh.getId());
      } else {
        if (neighbours.get(neigh.getId()).getLastSeen() < neigh.getLastSeen())
          neighbours.get(neigh.getId()).updateLastSeen(neigh.getLastSeen());
      }
    }
  }

  public void addNodes(BigInteger[] nodes) {

    for (BigInteger id : nodes) {
      if (id.compareTo(builderAddress) != 0) {
        if (!blackList.contains(id)
            && !validatorsIndexed.contains(id)
            && !builderAddress.equals(id)) {
          nonValidatorsIndexed.add(id);
        }
      }
    }
  }

  public void addValidatorNodes(BigInteger[] nodes) {
    for (BigInteger id : nodes) {
      if (!blackList.contains(id) && id.compareTo(builderAddress) != 0) {
        validatorsIndexed.add(id);
      }
    }
  }

  public void setBuilderAddress(BigInteger builderAddress) {
    this.builderAddress = builderAddress;
  }

  public void removeNode(BigInteger node) {
    this.nodesIndexed.remove(node);
    this.nonValidatorsIndexed.remove(node);
    this.neighbours.remove(node);
    validatorsIndexed.remove(node);
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

  public List<BigInteger> getAllNeighbours() {

    List<BigInteger> result = new ArrayList<>(neighbours.keySet());
    return result;
  }

  public Neighbour[] getNeighbours(int n) {

    List<Neighbour> result = new ArrayList<>();
    List<Neighbour> neighs = new ArrayList<>();
    for (Neighbour neigh : neighbours.values()) {
      neighs.add(neigh);
    }
    Collections.shuffle(neighs);

    for (Neighbour neigh : neighs) {
      if (result.size() < n) result.add(neigh);
      else break;
    }
    return result.toArray(new Neighbour[0]);
  }

  public void setEvilIds(List<Node> ids) {
    this.evilIds = ids;
  }

  public Neighbour[] getEvilNeighbours(int n) {

    List<Neighbour> result = new ArrayList<>();
    if (evilIds != null) {
      Collections.shuffle(evilIds);
      for (Node neigh : evilIds) {
        if (result.size() < n)
          result.add(new Neighbour(neigh.getDASProtocol().getKademliaId(), neigh, true));
        else break;
      }
    }
    return result.toArray(new Neighbour[0]);
  }

  public Neighbour[] getNeighbours(BigInteger id, BigInteger radius) {

    List<BigInteger> nodes = getNodesbySample(id, radius);
    List<Neighbour> neighs = new ArrayList<>();
    List<Neighbour> result = new ArrayList<>();
    for (BigInteger n : nodes) {
      neighs.add(neighbours.get(n));
    }
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

  public boolean isNeighbourKnown(Neighbour neighbour) {
    return neighbours.containsKey(neighbour.getId());
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
