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
// import peersim.kademlia.das.SearchTable;
import peersim.kademlia.das.SearchTable;

/**
 * This class represents a random sampling operation that collects samples from random nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class RandomSamplingOperation extends SamplingOperation {

  protected Block currentBlock;
  /**
   * default constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public RandomSamplingOperation(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block currentBlock,
      SearchTable searchTable,
      boolean isValidator,
      int numValidators,
      MissingNode callback) {
    super(srcNode, destNode, timestamp, currentBlock, isValidator, numValidators, callback);
    this.currentBlock = currentBlock;
    this.searchTable = searchTable;

    Sample[] randomSamples = currentBlock.getNRandomSamples(KademliaCommonConfigDas.N_SAMPLES);
    for (Sample rs : randomSamples) {
      FetchingSample s = new FetchingSample(rs);
      samples.put(rs.getIdByRow(), s);
      // samples.put(rs.getIdByColumn(), s);
    }
    createNodes();
  }

  public boolean completed() {

    boolean completed = true;
    for (FetchingSample s : samples.values()) {
      if (!s.isDownloaded()) {
        completed = false;
        break;
      }
    }
    return completed;
  }

  public void elaborateResponse(Sample[] sam) {

    for (Sample s : sam) {
      if (samples.containsKey(s.getId()) || samples.containsKey(s.getIdByColumn())) {
        FetchingSample fs = samples.get(s.getId());
        if (!fs.isDownloaded()) {
          samplesCount++;
          fs.setDownloaded();
        }
      }
    }
    // System.out.println("Samples received " + samplesCount);
  }

  public void elaborateResponse(Sample[] sam, BigInteger node) {
    this.available_requests--;
    // if (this.available_requests == 0) nodes.clear();

    Node n = nodes.get(node);
    if (n != null) {
      for (FetchingSample s : n.getSamples()) {
        s.removeFetchingNode(n);
      }
    }

    for (Sample s : sam) {

      if (samples.containsKey(s.getId()) || samples.containsKey(s.getIdByColumn())) {
        FetchingSample fs = samples.get(s.getId());
        if (!fs.isDownloaded()) {
          samplesCount++;
          fs.setDownloaded();
          fs.removeFetchingNode(nodes.get(node));
        }
      }
    }

    nodes.remove(node);
    askNodes.add(node);
  }

  protected void createNodes() {
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample).isDownloaded()) {

        List<BigInteger> validatorsBySampleRow =
            SearchTable.getNodesBySample(samples.get(sample).getId());
        List<BigInteger> validatorsBySampleColumn =
            SearchTable.getNodesBySample(samples.get(sample).getIdByColumn());

        List<BigInteger> validatorsBySample = new ArrayList<>();

        validatorsBySample.addAll(validatorsBySampleRow);
        validatorsBySample.addAll(validatorsBySampleColumn);

        List<BigInteger> nonValidatorsBySample = new ArrayList<>();
        nonValidatorsBySample.addAll(
            searchTable.getNonValidatorNodesbySample(
                samples.get(sample).getId(), radiusNonValidator));
        nonValidatorsBySample.addAll(
            searchTable.getNonValidatorNodesbySample(
                samples.get(sample).getIdByColumn(), radiusNonValidator));

        boolean found = false;

        if (validatorsBySampleRow != null && validatorsBySampleRow.size() > 0) {
          for (BigInteger id : validatorsBySampleRow) {
            if (!nodes.containsKey(id)) {
              nodes.put(id, new Node(id));
              nodes.get(id).addSample(samples.get(sample));
            } else {
              nodes.get(id).addSample(samples.get(sample));
            }
          }
          found = true;
        }

        if (nonValidatorsBySample != null && nonValidatorsBySample.size() > 0) {
          for (BigInteger id : nonValidatorsBySample) {
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

  public Map<String, Object> toMap() {
    // System.out.println("Mapping");
    Map<String, Object> result = new HashMap<String, Object>();

    result.put("id", this.operationId);
    result.put("src", this.srcNode);
    result.put("type", "RandomSamplingOperation");
    result.put("messages", getMessagesString());
    result.put("num_messages", getMessages().size());
    result.put("start", this.timestamp);
    result.put("completion_time", this.stopTime);
    result.put("hops", this.nrHops);
    result.put("samples", this.samplesCount);
    result.put("block_id", this.currentBlock.getBlockId());
    if (isValidator) result.put("validator", "yes");
    else result.put("validator", "no");
    if (completed()) result.put("completed", "yes");
    else result.put("completed", "no");
    return result;
  }
}
