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
      // samples.put(rs.getIdByColumn(), s);
    }
    // createNodes();
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
      if (samples.containsKey(s.getId()) /*|| samples.containsKey(s.getIdByColumn())*/) {
        FetchingSample fs = samples.get(s.getId());
        if (fs != null) {
          if (!fs.isDownloaded()) {
            samplesCount++;
            fs.setDownloaded();
          }
        }
      }
    }
    // System.out.println("Samples received " + samplesCount);
  }

  public void elaborateResponse(Sample[] sam, BigInteger node) {
    // this.available_requests--;
    // if (this.available_requests == 0) nodes.clear();
    pendingNodes.remove(node);
    Node n = nodes.get(node);
    if (n != null) {
      for (FetchingSample s : n.getSamples()) {
        s.removeFetchingNode(n);
        // s.setDownloaded();
      }
    }

    if (sam != null) {
      for (Sample s : sam) {
        if (samples.containsKey(s.getId()) /*|| samples.containsKey(s.getIdByColumn())*/) {
          FetchingSample fs = samples.get(s.getId());
          // FetchingSample fs2 = samples.get(s.getIdByColumn());
          if (fs != null) {
            if (!fs.isDownloaded()) {
              samplesCount++;
              fs.setDownloaded();
              fs.removeFetchingNode(nodes.get(node));
            }
          } /*else if(fs2!=null){
               if(!fs2.isDownloaded()){
            samplesCount++;
            fs2.setDownloaded();
            fs2.removeFetchingNode(nodes.get(node));
               }

             }*/
        }
      }
    }
    nodes.remove(node);
    // askedNodes.add(node);
  }

  public BigInteger[] getSamples() {
    List<BigInteger> result = new ArrayList<>();

    for (FetchingSample sample : samples.values()) {
      if (!sample.isDownloaded()) result.add(sample.getId());
    }

    return result.toArray(new BigInteger[0]);
  }

  protected void createNodes() {
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample).isDownloaded()) {

        List<BigInteger> nodesBySample = new ArrayList<>();

        BigInteger radiusUsed = radiusValidator;

        boolean notInRegion = false;
        while (!notInRegion) {
          nodesBySample.addAll(
              searchTable.getValidatorNodesbySample(samples.get(sample).getId(), radiusUsed));
          nodesBySample.addAll(
              searchTable.getValidatorNodesbySample(
                  samples.get(sample).getIdByColumn(), radiusUsed));
          if (!nodesBySample.isEmpty()) notInRegion = true;
          if (!notInRegion) radiusUsed = radiusUsed.multiply(BigInteger.valueOf(2));
        }
        nodesBySample.addAll(
            searchTable.getNonValidatorNodesbySample(
                samples.get(sample).getId(), radiusNonValidator));
        nodesBySample.addAll(
            searchTable.getNonValidatorNodesbySample(
                samples.get(sample).getIdByColumn(), radiusNonValidator));

        // boolean found = false;
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
          // found = true;
        }

        /*if (!found && callback != null) {
          callback.missing(sample, this);
        }*/
      }
    }
  }

  protected void addExtraNodes() {
    aggressiveness_step = KademliaCommonConfigDas.aggressiveness_step * 2;
    if (CommonState.getTime() - this.getTimestamp() > 1000)
      aggressiveness_step = KademliaCommonConfigDas.aggressiveness_step * 4;
    if (CommonState.getTime() - this.getTimestamp() > 2000)
      aggressiveness_step = KademliaCommonConfigDas.aggressiveness_step * 8;
    List<BigInteger> missingSamples = Arrays.asList(getSamples());
    Collections.shuffle(missingSamples);
    for (BigInteger sample : missingSamples) {
      int count = 0;
      List<BigInteger> nodesBySample = new ArrayList<>();
      BigInteger radiusUsed = radiusValidator;
      Sample[] sRow = currentBlock.getSamplesByRow(samples.get(sample).getSample().getRow());
      List<BigInteger> nodesToAdd = new ArrayList<>();
      for (Sample s : sRow) {
        // nodesToAdd.addAll(searchTable.getNodesbySample(s.getId(), radiusUsed));
        nodesToAdd.addAll(searchTable.getValidatorNodesbySample(s.getId(), radiusUsed));
      }
      Sample[] sColumn =
          currentBlock.getSamplesByColumn(samples.get(sample).getSample().getColumn());
      for (Sample s : sColumn) {
        // nodesToAdd.addAll(searchTable.getNodesbySample(s.getIdByColumn(), radiusUsed));
        nodesToAdd.addAll(searchTable.getValidatorNodesbySample(s.getId(), radiusUsed));
      }
      nodesBySample.addAll(new ArrayList<>(new LinkedHashSet<>(nodesToAdd)));

      nodesBySample.removeAll(askedNodes);
      if (nodesBySample != null && nodesBySample.size() > 0) {
        for (BigInteger id : nodesBySample) {
          if (!nodes.containsKey(id)) {
            nodes.put(id, new Node(id));
            nodes.get(id).addSample(samples.get(sample));
          } else {
            nodes.get(id).addSample(samples.get(sample));
          }
          count++;
          if (count == aggressiveness) return;
        }
      }
    }

    /*if (nodes.size() == 0 && getSamples().length > 0) {
      List<Sample> randomSamples = new ArrayList<>();
      while (randomSamples.size() < getSamples().length * 10) {
        Sample[] sample = currentBlock.getNRandomSamples(1);
        if (!Arrays.asList(getSamples()).contains(sample[0].getId())) randomSamples.add(sample[0]);
      }
      for (BigInteger id : getSamples()) samples.remove(id);
      askedNodes.clear();
      for (Sample rs : randomSamples) {
        FetchingSample s = new FetchingSample(rs);
        samples.put(rs.getIdByRow(), s);
      }
      createNodes();
    }*/
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
