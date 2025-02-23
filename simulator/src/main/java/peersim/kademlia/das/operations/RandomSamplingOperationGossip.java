package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.Sample;
import peersim.kademlia.gossipsub.GossipSubProtocol;

/**
 * This class represents a random sampling operation that collects samples from random nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class RandomSamplingOperationGossip extends SamplingOperation {
  BigInteger builderAddress;
  /**
   * default constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public RandomSamplingOperationGossip(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block currentBlock,
      boolean isValidator,
      int numValidators,
      BigInteger builderAddress) {
    super(srcNode, destNode, timestamp, currentBlock, isValidator, numValidators);
    this.builderAddress = builderAddress;
    Sample[] randomSamples = currentBlock.getNRandomSamples(KademliaCommonConfigDas.N_SAMPLES);
    for (Sample rs : randomSamples) {
      FetchingSample s = new FetchingSample(rs);
      samples.put(rs.getIdByRow(), s);
    }
  }

  public void createNodes() {
    for (BigInteger sample : samples.keySet()) {
      boolean found = false;
      if (!samples.get(sample).isDownloaded()) {
        HashSet<BigInteger> validatorsBySampleRow =
            GossipSubProtocol.getTable().getPeers("Row" + samples.get(sample).getRow());
        HashSet<BigInteger> validatorsBySampleColumn =
            GossipSubProtocol.getTable().getPeers("Column" + samples.get(sample).getColumn());
        validatorsBySampleRow.remove(builderAddress);
        validatorsBySampleColumn.remove(builderAddress);

        List<BigInteger> validatorsBySample = new ArrayList<>();

        validatorsBySample.addAll(validatorsBySampleRow);
        validatorsBySample.addAll(validatorsBySampleColumn);

        validatorsBySample.remove(builderAddress);

        if (validatorsBySample != null && validatorsBySample.size() > 0) {
          found = true;
          for (BigInteger id : validatorsBySample) {
            if (!nodes.containsKey(id)) {
              nodes.put(id, new Node(id));
              nodes.get(id).addSample(samples.get(sample));
            } else {
              nodes.get(id).addSample(samples.get(sample));
            }
          }
        }
        if (!found) {
          throw new UnsupportedOperationException("no validators found");
        }
      }
    }
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

  public BigInteger[] getSamples() {
    List<BigInteger> result = new ArrayList<>();

    for (FetchingSample sample : samples.values()) {
      if (!sample.isDownloaded()) result.add(sample.getId());
    }

    return result.toArray(new BigInteger[0]);
  }

  protected void addExtraNodes() {}

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

  public Map<String, Object> toMap() {
    // System.out.println("Mapping");
    Map<String, Object> result = new HashMap<String, Object>();

    result.put("id", this.operationId);
    result.put("src", this.srcNode);
    result.put("type", "RandomSamplingOperation");
    result.put("messages", getMessagesString());
    result.put("nodes_contacted", getMessages().size());
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
