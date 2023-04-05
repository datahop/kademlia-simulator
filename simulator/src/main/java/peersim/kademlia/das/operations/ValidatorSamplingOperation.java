package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  private HashMap<BigInteger, Boolean> row;
  private HashMap<BigInteger, Boolean> column;
  private boolean completed;
  /**
   * default constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public ValidatorSamplingOperation(
      BigInteger srcNode, BigInteger destNode, SearchTable rou, long timestamp, Block block) {
    super(srcNode, destNode, timestamp);
    row = new HashMap<>();
    column = new HashMap<>();
    for (BigInteger sample : block.getSamplesIdsByColumn(CommonState.r.nextInt(block.getSize()))) {
      column.put(sample, false);
    }
    for (BigInteger sample : block.getSamplesIdsByRow(CommonState.r.nextInt(block.getSize()))) {
      row.put(sample, false);
    }
    completed = false;
    for (BigInteger id : rou.getAllNeighbours()) {
      BigInteger[] samples = getSamples(block, id);
      for (BigInteger s : samples) {
        if (row.containsKey(s) || row.containsKey(s)) closestSet.put(id, false);
      }
    }
  }

  public BigInteger getNeighbour() {

    BigInteger res = null;

    if (closestSet.size() > 0) {
      BigInteger[] results = (BigInteger[]) closestSet.keySet().toArray(new BigInteger[0]);
      res = results[CommonState.r.nextInt(results.length)];
    }

    if (res != null) {
      closestSet.remove(res);
      // closestSet.put(res, true);
      // increaseUsed(res);
      this.available_requests--; // decrease available request
    }
    return res;
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (row.containsKey(s.getId())) {
        row.remove(s.getId());
        row.put(s.getId(), true);
      }
      if (column.containsKey(s.getId())) {
        column.remove(s.getId());
        column.put(s.getId(), true);
      }
    }

    int countRow = 0, countColumn = 0;
    for (Boolean r : row.values()) {
      if (r) countRow++;
    }
    for (Boolean r : column.values()) {
      if (r) countColumn++;
    }
    if (countColumn > column.size() / 2 && countRow > row.size() / 2) completed = true;
  }

  public BigInteger[] getSamples(Block b, BigInteger peerId) {

    return b.getSamplesByRadius(
        peerId, b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));
  }

  /*public List<BigInteger> getReceivedSamples() {
    return samples;
  }*/

  public boolean completed() {
    // System.out.println("Samples num " + samples.size());

    return completed;
  }

  public BigInteger[] startSampling() {
    setAvailableRequests(KademliaCommonConfig.ALPHA);

    List<BigInteger> nextNodes = new ArrayList<>();
    // send ALPHA messages
    for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
      BigInteger nextNode = getNeighbour();
      if (nextNode != null) {
        nextNodes.add(nextNode);
      }
    }
    if (nextNodes.size() > 0) return nextNodes.toArray(new BigInteger[0]);
    return new BigInteger[0];
  }

  public BigInteger[] continueSampling() {

    List<BigInteger> nextNodes = new ArrayList<>();

    System.out.println("continueSampling " + getAvailableRequests());
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

  @Override
  public void addNodes(List<BigInteger> nodes) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'addNodes'");
  }
}
