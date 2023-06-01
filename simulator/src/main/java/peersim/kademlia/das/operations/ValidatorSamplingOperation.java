package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;

/**
 * This class represents the validator sampling operation that collects row or columns of samples
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class ValidatorSamplingOperation extends SamplingOperation {

  // private RoutingTable rou;
  protected int row, column;
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
      int column,
      boolean isValidator,
      MissingNode callback) {
    super(srcNode, null, timestamp, block, isValidator, callback);

    // System.out.println("Row " + row + " column " + column);
    assert (row == 0 || column == 0) : "Either row or column should be set";
    assert (!(row == 0 && column == 0)) : "Both row or column are set";

    this.row = row;
    this.column = column;
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
    setAvailableRequests(KademliaCommonConfigDas.ALPHA);
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (row > 0) {
        if (samples.containsKey(s.getIdByRow())) {
          if (!samples.get(s.getIdByRow())) {
            samplesCount++;
            samples.remove(s.getIdByRow());
            samples.put(s.getIdByRow(), true);
          }
        }
      } else {
        if (samples.containsKey(s.getIdByColumn())) {
          if (!samples.get(s.getIdByColumn())) {
            samplesCount++;
            samples.remove(s.getIdByColumn());
            samples.put(s.getIdByColumn(), true);
          }
        }
      }
    }
    // System.out.println("Row " + samplesCount + " " + samples.size());
    if (samplesCount >= samples.size() / 2) completed = true;
  }

  /*public BigInteger[] getSamples(BigInteger peerId) {

    List<BigInteger> list = new ArrayList<>();
    if (row > 0) {
      Collections.addAll(
          list,
          currentBlock.getSamplesByRadiusByRow(
              peerId,
              currentBlock.computeRegionRadius(
                  KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));
    } else if (column > 0) {
      Collections.addAll(
          list,
          currentBlock.getSamplesByRadiusByColumn(
              peerId,
              currentBlock.computeRegionRadius(
                  KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));
    }

    List<BigInteger> result = new ArrayList<>();

    for (BigInteger s : samples.keySet()) {
      if (samples.get(s) != null) {
        if (!samples.get(s) && list.contains(s)) result.add(s);
      }
    }
    return result.toArray(new BigInteger[0]);
  }*/

  public boolean completed() {

    return completed;
  }

  public BigInteger[] doSampling() {

    List<BigInteger> nextNodes = new ArrayList<>();

    while ((getAvailableRequests() > 0)) { // I can send a new find request

      // get an available neighbour
      BigInteger nextNode = getNeighbour();
      if (nextNode != null) {
        nextNodes.add(nextNode);
      } else {
        break;
      }
    }

    if (nextNodes.size() > 0) return nextNodes.toArray(new BigInteger[0]);
    else return new BigInteger[0];
  }

  public int getRow() {
    return row;
  }

  public int getColumn() {
    return column;
  }

  public Map<String, Object> toMap() {
    // System.out.println("Mapping");
    Map<String, Object> result = new HashMap<String, Object>();

    result.put("id", this.operationId);
    result.put("src", this.srcNode);
    result.put("type", "ValidatorSamplingOperation");
    result.put("messages", getMessagesString());
    result.put("start", this.timestamp);
    result.put("completion_time", this.stopTime);
    result.put("hops", this.nrHops);
    result.put("samples", this.samplesCount);
    result.put("row", this.row);
    result.put("column", this.column);
    result.put("block_id", this.currentBlock.getBlockId());
    if (isValidator) result.put("validator", "yes");
    else result.put("validator", "no");
    if (completed) result.put("completed", "yes");
    else result.put("completed", "no");
    return result;
  }
}
