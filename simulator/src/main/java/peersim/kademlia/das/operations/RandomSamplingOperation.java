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
    }
    createNodes();
  }

  public boolean completed() {

    boolean completed = true;
    int failed = 0;
    for (FetchingSample s : samples.values()) {
      if (!s.isDownloaded()) {
        failed++;
        if (failed > KademliaCommonConfigDas.MAX_SAMPLING_FAILED) {
          completed = false;
          break;
        }
      }
    }
    return completed;
  }

  public void elaborateResponse(Sample[] sam) {

    for (Sample s : sam) {
      if (samples.containsKey(s.getId())) {
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

      if (samples.containsKey(s.getId())) {
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

        List<BigInteger> nodesBySample = new ArrayList<>();

        List<BigInteger> idsValidatorsRows =
            searchTable.getValidatorNodesbySample(samples.get(sample).getId(), radiusValidator);
        List<BigInteger> idsValidatorsColumns =
            searchTable.getValidatorNodesbySample(
                samples.get(sample).getIdByColumn(), radiusValidator);

        List<BigInteger> idsNonValidatorsRows =
            searchTable.getNonValidatorNodesbySample(
                samples.get(sample).getId(), radiusNonValidator);
        List<BigInteger> idsNonValidatorsColumns =
            searchTable.getNonValidatorNodesbySample(
                samples.get(sample).getIdByColumn(), radiusNonValidator);

        nodesBySample.addAll(idsValidatorsRows);
        nodesBySample.addAll(idsValidatorsColumns);

        nodesBySample.addAll(idsNonValidatorsRows);
        nodesBySample.addAll(idsNonValidatorsColumns);
        boolean found = false;

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

        if (!found && callback != null) {
          callback.missing(sample, this);
        }
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
