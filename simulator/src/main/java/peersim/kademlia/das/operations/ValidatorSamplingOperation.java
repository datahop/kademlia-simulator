package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
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

  private HashMap<BigInteger, Boolean> row;
  private HashMap<BigInteger, Boolean> column;
  private boolean completed;
  // private RoutingTable rou;
  private BigInteger rowId, columnId;
  private Block currentBlock;
  private int countColumn = 0, countRow = 0;
  /**
   * default constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public ValidatorSamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block block,
      SearchTable searchTable) {
    super(srcNode, destNode, timestamp);
    row = new HashMap<>();
    column = new HashMap<>();
    currentBlock = block;
    for (BigInteger sample : block.getSamplesIdsByColumn(CommonState.r.nextInt(block.getSize()))) {
      column.put(sample, false);
    }
    for (BigInteger sample : block.getSamplesIdsByRow(CommonState.r.nextInt(block.getSize()))) {
      row.put(sample, false);
    }
    setAvailableRequests(KademliaCommonConfig.ALPHA);
    // System.out.println("Rows " + row.size() + " columns" + column.size());
    // rou = new RoutingTable(KademliaCommonConfig.NBUCKETS, 0, 0);
    completed = false;
    /*for (BigInteger id : rou.getAllNeighbours()) {
      BigInteger[] samples = getSamples(block, id);
      for (BigInteger s : samples) {
        if (row.containsKey(s) || column.containsKey(s)) closestSet.put(id, false);
      }
    }*/
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

  public Set<BigInteger> getRow() {
    return row.keySet();
  }

  public Set<BigInteger> getColumn() {
    return column.keySet();
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (row.containsKey(s.getId())) {
        if (!row.get(s.getId())) {
          countRow++;
          row.remove(s.getId());
          row.put(s.getId(), true);
        }
      }
      if (column.containsKey(s.getId())) {
        if (!column.get(s.getId())) {
          countColumn++;
          column.remove(s.getId());
          column.put(s.getId(), true);
        }
      }
    }
    System.out.println("Row " + countRow + " column " + countColumn);
    if (countColumn > column.size() / 2 && countRow > row.size() / 2) completed = true;
  }

  public BigInteger[] getAnySample() {
    List<BigInteger> list = new ArrayList<>();
    for (BigInteger id : column.keySet()) if (!column.get(id)) list.add(id);
    for (BigInteger id : row.keySet()) if (!row.get(id)) list.add(id);

    return list.toArray(new BigInteger[0]);
  }

  public BigInteger[] getSamples(BigInteger peerId) {

    return currentBlock.getSamplesByRadius(
        peerId,
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));
  }

  /*public List<BigInteger> getReceivedSamples() {
    return samples;
  }*/

  public boolean completed() {
    // System.out.println("Samples num " + samples.size());

    return completed;
  }

  /*public BigInteger[] startSampling() {
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
  }*/

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
