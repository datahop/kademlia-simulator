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

// SearchTable class is used by nodes as a local store to index all peers in the network discovered
public class SearchTable {

  // Used to send known nodes to other peers
  private HashMap<BigInteger, Neighbour> neighbours;

  // Used for local sampling
  private TreeSet<BigInteger> nodesIndexed;
  private static TreeSet<BigInteger> validatorsIndexed = new TreeSet<>();
  private TreeSet<BigInteger> nonValidatorsIndexed;

  // Builder node
  private BigInteger builderAddress;

  private HashSet<BigInteger> blackList;
  private List<Node> evilNodes;
  private List<BigInteger> evilIds;
  private boolean onlyAddEvilNghbrs = false;

  public SearchTable() {

    this.nodesIndexed = new TreeSet<>();
    this.nonValidatorsIndexed = new TreeSet<>();
    this.blackList = new HashSet<>();
    this.neighbours = new HashMap<>();
  }

  public void addNeighbour(Neighbour neigh) {
    if (this.onlyAddEvilNghbrs && !neigh.isEvil()) {
      return;
    }

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

  public void setOnlyAddEvilNghbrs() {
    this.onlyAddEvilNghbrs = true;
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

  public void seenNeighbour(BigInteger id, Node n) {
    if (id.compareTo(builderAddress) != 0) {
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

  public void setEvil(List<Node> nodes) {
    this.evilNodes = nodes;
  }

  public boolean isEvil(BigInteger id) {
    if (evilIds.contains(id)) return true;
    else return false;
  }

  public void setEvilIds(List<BigInteger> ids) {
    this.evilIds = ids;
  }

  public Neighbour[] getNeighbours() {

    List<Neighbour> result = new ArrayList<>();
    List<Neighbour> neighs = new ArrayList<>();
    for (Neighbour n : neighbours.values()) {
      neighs.add(n);
    }
    Collections.sort(neighs);

    for (Neighbour neigh : neighs) {
      if (result.size() < KademliaCommonConfigDas.MAX_NODES_RETURNED) result.add(neigh);
      else break;
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
    Collections.sort(neighs);

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

  public Neighbour[] getEvilNeighbours(int n) {

    List<Neighbour> result = new ArrayList<>();
    if (evilNodes != null) {
      Collections.shuffle(evilNodes);
      for (Node neigh : evilNodes) {
        if (result.size() < n)
          result.add(new Neighbour(neigh.getDASProtocol().getKademliaId(), neigh, true));
        else break;
      }
    }
    return result.toArray(new Neighbour[0]);
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
