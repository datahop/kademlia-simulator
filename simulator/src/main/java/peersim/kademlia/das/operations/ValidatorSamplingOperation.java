package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import peersim.core.CommonState;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;

/**
 * This class represents a random sampling operation that collects samples from random nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class ValidatorSamplingOperation extends SamplingOperation {

  private HashMap<BigInteger, Boolean> samples;
  private boolean completed;
  // private RoutingTable rou;
  private Block currentBlock;
  private int samplesCount = 0;
  private SearchTable searchTable;

  /**
   * default constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public ValidatorSamplingOperation(
      BigInteger srcNode,
      long timestamp,
      Block block,
      SearchTable searchTable,
      int row,
      int column) {
    super(srcNode, null, timestamp);
    samples = new HashMap<>();
    currentBlock = block;

    System.out.println("Row " + row + " column " + column);
    assert (row == 0 || column == 0) : "Either row or column should be set";
    assert (!(row == 0 && column == 0)) : "Both row or column are set";

    if (row > 0) {
      for (BigInteger sample : block.getSamplesIdsByRow(row)) {
        samples.put(sample, false);
      }
    } else if (column > 0) {
      for (BigInteger sample : block.getSamplesIdsByColumn(column)) {
        samples.put(sample, false);
      }
    }
    this.searchTable = searchTable;
    setAvailableRequests(KademliaCommonConfig.ALPHA);
    completed = false;
  }

  public BigInteger getNeighbour() {

    BigInteger res = null;
    List<BigInteger> nodes = new ArrayList<>();
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample)) {
        List<BigInteger> nodesBySample = searchTable.getNodesbySample(sample);
        if (nodesBySample != null) nodes.addAll(nodesBySample);
      }
    }
    // System.out.println("Get neighbour " + nodes.size());
    if (nodes.size() > 0) {
      // while (closestSet.get(res) != null)
      res = nodes.get(CommonState.r.nextInt(nodes.size()));
      while (closestSet.get(res) != null && nodes.size() > 0) {
        res = nodes.get(CommonState.r.nextInt(nodes.size()));
        nodes.remove(res);
      }
    }

    if (res != null) {
      // closestSet.remove(res);
      closestSet.put(res, true);
      // increaseUsed(res);
      this.available_requests--; // decrease available request
    }
    return res;
  }

  public Set<BigInteger> getSamples() {
    return samples.keySet();
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (samples.containsKey(s.getId())) {
        if (!samples.get(s.getId())) {
          samplesCount++;
          samples.remove(s.getId());
          samples.put(s.getId(), true);
        }
      }
    }
    System.out.println("Row " + samplesCount + " " + samples.size());
    if (samplesCount > samples.size() / 2) completed = true;
  }

  public BigInteger[] getSamples(BigInteger peerId) {

    /*return currentBlock.getSamplesByRadius(
    peerId,
    currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));*/
    List<BigInteger> result = new ArrayList<>();
    /*Collections.addAll(
    result,
    currentBlock.getSamplesByRadiusByColumn(
        peerId,
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));*/
    Collections.addAll(
        result,
        currentBlock.getSamplesByRadiusByRow(
            peerId,
            currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));

    return result.toArray(new BigInteger[0]);
  }

  public boolean completed() {

    return completed;
  }

  public int samplesCount() {
    return samplesCount;
  }

  public BigInteger[] doSampling() {

    List<BigInteger> nextNodes = new ArrayList<>();

    // System.out.println("continueSampling " + getAvailableRequests() + " " + closestSet.size());
    while (getAvailableRequests() > 0) { // I can send a new find request

      // get an available neighbour
      BigInteger nextNode = getNeighbour();
      if (nextNode != null) {
        nextNodes.add(nextNode);
      } else {
        break;
      }
    }
    if (nextNodes.size() > 0) return nextNodes.toArray(new BigInteger[0]);
    return new BigInteger[0];
  }
}
