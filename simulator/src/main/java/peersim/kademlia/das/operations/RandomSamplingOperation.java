package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
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

  private Block currentBlock;
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
      boolean isValidator) {
    super(srcNode, destNode, timestamp, isValidator);
    setAvailableRequests(KademliaCommonConfig.ALPHA);
    this.currentBlock = currentBlock;
    this.searchTable = searchTable;

    Sample[] randomSamples = currentBlock.getNRandomSamples(KademliaCommonConfigDas.N_SAMPLES);
    for (Sample rs : randomSamples) {
      samples.put(rs.getId(), false);
      samples.put(rs.getIdByColumn(), false);
    }
  }

  public BigInteger[] getSamples() {
    List<BigInteger> result = new ArrayList<>();

    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample)) result.add(sample);
    }

    return result.toArray(new BigInteger[0]);
  }

  public BigInteger[] getSamples(BigInteger peerId) {
    return getSamples();
  }

  public boolean completed() {

    for (boolean found : samples.values()) if (!found) return false;

    return true;
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

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (samples.containsKey(s.getId()) || samples.containsKey(s.getIdByColumn())) {
        if (!samples.get(s.getId()) || !samples.get(s.getIdByColumn())) {
          samples.remove(s.getId());
          samples.remove(s.getIdByColumn());
          samples.put(s.getIdByColumn(), true);
          samples.put(s.getId(), true);
          samplesCount++;
        }
      }
    }
    System.out.println("Samples received " + samples.size());
  }

  public Map<String, Object> toMap() {
    // System.out.println("Mapping");
    Map<String, Object> result = new HashMap<String, Object>();

    result.put("id", this.operationId);
    result.put("src", this.srcNode);
    result.put("type", this.getClass().getSimpleName());
    result.put("messages", this.messages);
    result.put("start", this.timestamp);
    result.put("stop", this.stopTime);
    result.put("hops", this.nrHops);
    result.put("samples", this.samplesCount);
    result.put("block_id", this.currentBlock.getBlockId());
    if(isValidator) result.put("validator","yes");
    else result.put("validator","no");
    if (completed) result.put("completed", "yes");
    else result.put("completed", "no");
    return result;
  }
}
