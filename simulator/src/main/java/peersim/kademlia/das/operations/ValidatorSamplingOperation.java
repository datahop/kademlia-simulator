package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import peersim.core.CommonState;
import peersim.kademlia.das.Block;
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
      int numValidators,
      MissingNode callback) {
    super(srcNode, null, timestamp, block, isValidator, numValidators, callback);

    // System.out.println("Row " + row + " column " + column);
    assert (row == 0 || column == 0) : "Either row or column should be set";
    assert (!(row == 0 && column == 0)) : "Both row or column are set";

    this.row = row;
    this.column = column;
    if (row > 0) {
      for (Sample sample : block.getSamplesByRow(row)) {
        samples.put(sample.getId(), new FetchingSample(sample));
      }
    } else if (column > 0) {
      for (Sample sample : block.getSamplesByColumn(column)) {
        samples.put(sample.getId(), new FetchingSample(sample));
      }
    }
    this.searchTable = searchTable;
    // createNodes();
  }

  public void elaborateResponse(Sample[] sam) {

    for (Sample s : sam) {
      // if (row > 0) {
      if (samples.containsKey(s.getId())) {
        FetchingSample fs = samples.get(s.getId());
        if (!fs.isDownloaded()) {
          fs.setDownloaded();
          samplesCount++;
        }
      }
      /*} else {
        if (samples.containsKey(s.getIdByColumn())) {
          FetchingSample fs = samples.get(s.getIdByColumn());
          if (!fs.isDownloaded()) {
            fs.setDownloaded();
            samplesCount++;
          }
        }
      }*/
    }
    /*System.out.println(
    "["
        + CommonState.getTime()
        + "]["
        + srcNode
        + "] Completed operation "
        + this.getId()
        + " "
        + samplesCount
        + " "
        + samples.size());*/
    if (samplesCount >= samples.size() / 2) completed = true;
  }

  public void elaborateResponse(Sample[] sam, BigInteger n) {

    // this.available_requests--;
    // if (this.available_requests == 0) nodes.clear();

    pendingNodes.remove(n);
    Node node = nodes.get(n);
    if (node != null) {
      for (FetchingSample s : node.getSamples()) {
        s.removeFetchingNode(node);
      }
    }
    if (sam != null) {
      for (Sample s : sam) {
        // if (row > 0) {
        if (samples.containsKey(s.getId())) {
          FetchingSample fs = samples.get(s.getId());
          if (!fs.isDownloaded()) {
            fs.setDownloaded();
            samplesCount++;
          }
        }
        /*} else {
          if (samples.containsKey(s.getIdByColumn())) {
            FetchingSample fs = samples.get(s.getIdByColumn());
            if (!fs.isDownloaded()) {
              fs.setDownloaded();
              samplesCount++;
            }
          }
        }*/
      }
    }
    /*System.out.println(
    "["
        + CommonState.getTime()
        + "]["
        + srcNode
        + "] Completed operation "
        + this.getId()
        + " "
        + samplesCount
        + " "
        + samples.size());*/
    // System.out.println("Row " + samplesCount + " " + samples.size());
    if (samplesCount >= samples.size() / 2) completed = true;

    // askedNodes.add(n);
    nodes.remove(n);
  }

  public boolean completed() {

    return completed;
  }

  public int getRow() {
    return row;
  }

  public int getColumn() {
    return column;
  }

  @Override
  protected void createNodes() {
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample).isDownloaded()) {

        List<BigInteger> nodesBySample = new ArrayList<>();
        if (row > 0) {
          BigInteger radiusUsed = radiusValidator;
          while (nodesBySample.isEmpty() && radiusUsed.compareTo(Block.MAX_KEY) == -1) {
            nodesBySample.addAll(
                searchTable.getNodesbySample(samples.get(sample).getId(), radiusUsed));
            radiusUsed = radiusUsed.multiply(BigInteger.valueOf(2));
          }
        } else {
          BigInteger radiusUsed = radiusValidator;
          while (nodesBySample.isEmpty() && radiusUsed.compareTo(Block.MAX_KEY) == -1) {
            nodesBySample.addAll(
                searchTable.getNodesbySample(samples.get(sample).getIdByColumn(), radiusUsed));

            radiusUsed = radiusUsed.multiply(BigInteger.valueOf(2));
          }
        }
        boolean found = false;
        nodesBySample.removeAll(askedNodes);
        if (nodesBySample != null && nodesBySample.size() > 0) {
          for (BigInteger id : nodesBySample) {
            if (!nodes.containsKey(id)) {
              nodes.put(id, new Node(id));
              nodes.get(id).addSample(samples.get(sample));
            } else {
              nodes.get(id).addSample(samples.get(sample));
            }
          }
          found = true;
        }

        if (!found && callback != null) callback.missing(sample, this);
      }
    }
  }

  public BigInteger[] getSamples() {
    List<BigInteger> result = new ArrayList<>();

    for (FetchingSample sample : samples.values()) {
      if (!sample.isDownloaded()) {
        // if (column > 0) result.add(sample.getIdByColumn());
        // else
        result.add(sample.getId());
      }
    }

    return result.toArray(new BigInteger[0]);
  }

  protected void addExtraNodes() {
    if (CommonState.getTime() - this.getTimestamp() > 1000)
      aggressiveness_step = aggressiveness_step * 2;
    List<BigInteger> missingSamples = Arrays.asList(getSamples());
    Collections.shuffle(missingSamples);
    for (BigInteger sample : missingSamples) {
      int count = 0;
      List<BigInteger> nodesBySample = new ArrayList<>();
      BigInteger radiusUsed = radiusValidator;
      if (column > 0) {
        Sample[] sRow = currentBlock.getSamplesByRow(samples.get(sample).getSample().getRow());
        List<BigInteger> nodes = new ArrayList<>();
        for (Sample s : sRow) {
          nodes.addAll(searchTable.getNodesbySample(s.getId(), radiusUsed));
        }

        nodesBySample.addAll(new ArrayList<>(new LinkedHashSet<>(nodes)));
      } else {
        Sample[] sColumn =
            currentBlock.getSamplesByColumn(samples.get(sample).getSample().getColumn());
        List<BigInteger> nodes = new ArrayList<>();
        for (Sample s : sColumn) {
          nodes.addAll(searchTable.getNodesbySample(s.getIdByColumn(), radiusUsed));
        }
        nodesBySample.addAll(new ArrayList<>(new LinkedHashSet<>(nodes)));
      }

      nodesBySample.removeAll(askedNodes);
      Collections.shuffle(nodesBySample);
      // int max = 0;
      if (nodesBySample != null && nodesBySample.size() > 0) {
        for (BigInteger id : nodesBySample) {
          if (!nodes.containsKey(id)) {
            nodes.put(id, new Node(id));
            nodes.get(id).addSample(samples.get(sample));
          } else {
            nodes.get(id).addSample(samples.get(sample));
          }
          count++;
          if (count > aggressiveness - 2) return;
        }
      }
    }
  }

  public Map<String, Object> toMap() {
    // System.out.println("Mapping");
    Map<String, Object> result = new HashMap<String, Object>();

    result.put("id", this.operationId);
    result.put("src", this.srcNode);
    result.put("type", "ValidatorSamplingOperation");
    result.put("messages", getMessagesString());
    result.put("num_messages", getMessages().size());
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
