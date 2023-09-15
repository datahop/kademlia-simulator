package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
// import peersim.kademlia.KademliaCommonConfig;

public class SearchTable {

  // private HashMap<BigInteger, List<BigInteger>> sampleMap;

  // private Block currentBlock;

  private TreeSet<BigInteger> nodesIndexed; // , samplesIndexed;

  private static TreeSet<BigInteger> validatorsIndexed = new TreeSet<>(); // , samplesIndexed;

  private TreeSet<BigInteger> nonValidatorsIndexed; // , samplesIndexed;

  private HashSet<BigInteger> blackList; // , samplesIndexed;

  // private static HashMap<BigInteger, List<BigInteger>> sampleMap = new HashMap<>();
  private BigInteger builderAddress;

  public SearchTable(/*Block currentblock , BigInteger id*/ ) {

    // this.currentBlock = currentblock;
    // this.sampleMap = new HashMap<>();
    this.nodesIndexed = new TreeSet<>();
    this.nonValidatorsIndexed = new TreeSet<>();

    this.blackList = new HashSet<>();

    // routingTable = new RoutingTable(KademliaCommonConfig.K, KademliaCommonConfig.BITS, 0);
    // routingTable.setNodeId(id);
  }

  /*public void setBlock(Block currentBlock) {
    this.currentBlock = currentBlock;
  }*/

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
      if (!blackList.contains(id)
          && !validatorsIndexed.contains(id)
          && !builderAddress.equals(id)) {
        nonValidatorsIndexed.add(id);
      }
    }
  }

  /*public void addNonValidatorNodes(BigInteger[] nodes) {

    for (BigInteger id : nodes) {
      if (!blackList.contains(id) && !validatorsIndexed.contains(id)) {
        nodesIndexed.add(id);
      }
    }
  }*/

  public void addValidatorNodes(BigInteger[] nodes) {
    for (BigInteger id : nodes) {
      if (!blackList.contains(id)) {
        validatorsIndexed.add(id);
      }
      // routingTable.addNeighbour(id);
    }
  }

  public void setBuilderAddress(BigInteger builderAddress) {
    this.builderAddress = builderAddress;
  }

  public void removeNode(BigInteger node) {
    // this.blackList.add(node);
    this.nodesIndexed.remove(node);
    // this.validatorsIndexed.remove(node);
    this.nonValidatorsIndexed.remove(node);
  }

  public TreeSet<BigInteger> nodesIndexed() {
    return nonValidatorsIndexed;
  }

  public TreeSet<BigInteger> getValidatorsIndexed() {
    return validatorsIndexed;
  }

  /*public HashSet<BigInteger> samplesIndexed() {
    return samplesIndexed;
  }*/

  public List<BigInteger> getNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;

    Collection<BigInteger> subSet = nodesIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);

    // return sampleMap.get(sampleId);

  }

  public List<BigInteger> getValidatorNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;
    Collection<BigInteger> subSet = validatorsIndexed.subSet(bottom, true, top, true);
    return new ArrayList<BigInteger>(subSet);

    // return sampleMap.get(sampleId);

  }

  public List<BigInteger> getNonValidatorNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger bottom = sampleId.subtract(radius);
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.ZERO;

    BigInteger top = sampleId.add(radius);
    if (top.compareTo(Block.MAX_KEY) == 1) top = Block.MAX_KEY;

    Collection<BigInteger> subSet = nonValidatorsIndexed.subSet(bottom, true, top, true);
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

  public void refresh() {

  }

  /*public static void createSampleMap(Block currentBlock) {

    int validatorParcelSize =
        (currentBlock.getSize()
                * currentBlock.getSize()
                * KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)
            / KademliaCommonConfigDas.validatorsSize;

    if (validatorParcelSize == 0) validatorParcelSize = 1;
    Random r = new Random();
    List<BigInteger> list = new ArrayList<BigInteger>(validatorsIndexed);

    List<BigInteger> sampleList = new ArrayList<>();

    // Assigning samples to node by row
    for (int h = 0; h < KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER; h++) {
      for (int i = 0; i < currentBlock.getSize(); i++) {
        for (int j = 0; j < currentBlock.getSize(); j++) {
          sampleList.add(currentBlock.getSample(i, j).getIdByRow());
          if (sampleList.size() == validatorParcelSize) {
            BigInteger node = list.get(r.nextInt(list.size()));
            // samples.put(node, sampleList);
            for (BigInteger sample : sampleList) {
              if (sampleMap.containsKey(sample)) {

                sampleMap.get(sample).add(node);
              } else {
                List<BigInteger> nList = new ArrayList<>();
                nList.add(node);
                sampleMap.put(sample, nList);
              }
            }
            sampleList.clear();
          }
        }
      }
    }
    if (sampleList.size() != 0) {
      BigInteger node = list.get(r.nextInt(list.size()));
      // samples.put(node, sampleList);
      for (BigInteger sample : sampleList) {
        if (sampleMap.containsKey(sample)) sampleMap.get(sample).add(node);
        else {
          List<BigInteger> nList = new ArrayList<>();
          nList.add(node);
          sampleMap.put(sample, nList);
        }
      }
      sampleList.clear();
    }

    // Assigning samples to node by column
    for (int h = 0; h < KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER; h++) {
      for (int i = 0; i < currentBlock.getSize(); i++) {
        for (int j = 0; j < currentBlock.getSize(); j++) {
          sampleList.add(currentBlock.getSample(j, i).getIdByColumn());
          if (sampleList.size() == validatorParcelSize) {
            BigInteger node = list.get(r.nextInt(list.size()));
            // samples.put(node, sampleList);
            for (BigInteger sample : sampleList) {
              // System.out.println("assign samples to node " + node + " " + nodeMap.size());
              if (sampleMap.containsKey(sample)) {

                sampleMap.get(sample).add(node);
              } else {
                List<BigInteger> nList = new ArrayList<>();
                nList.add(node);
                sampleMap.put(sample, nList);
              }
            }
            sampleList.clear();
          }
        }
      }
    }
    if (sampleList.size() != 0) {
      BigInteger node = list.get(r.nextInt(list.size()));
      // samples.put(node, sampleList);
      for (BigInteger sample : sampleList) {
        if (sampleMap.containsKey(sample)) sampleMap.get(sample).add(node);
        else {
          List<BigInteger> nList = new ArrayList<>();
          nList.add(node);
          sampleMap.put(sample, nList);
        }
      }
      sampleList.clear();
    }

    System.out.println(
        "samples "
            + validatorParcelSize
            + " "
            + sampleMap.size()
            + " "
            + sampleList.size()
            + " "
            + currentBlock.getSize());

    //for (List<BigInteger> l : sampleMap.values()) {
    //  System.out.println("samples " + l.size());
    //}
  }*/

  /*public static List<BigInteger> getNodesBySample(BigInteger s) {
    return sampleMap.get(s);
  }

  public BigInteger[] findKClosestValidators(BigInteger sampleId) {

    return routingTable.getNeighbours(sampleId, sampleId);
  }*/
}
