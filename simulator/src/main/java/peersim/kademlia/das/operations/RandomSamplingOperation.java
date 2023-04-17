package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
      SearchTable searchTable) {
    super(srcNode, destNode, timestamp);
    setAvailableRequests(KademliaCommonConfig.ALPHA);
    this.currentBlock = currentBlock;
    this.searchTable = searchTable;

    Sample[] randomSamples = currentBlock.getNRandomSamples(KademliaCommonConfigDas.N_SAMPLES);
    for (Sample rs : randomSamples) {
      samples.put(rs.getId(), false);
    }

    // for (BigInteger id : rou.getAllNeighbours()) closestSet.put(id, false);
  }

  public BigInteger[] getSamples(BigInteger peerId) {

    /*return currentBlock.getSamplesByRadius(
    peerId,
    currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));*/
    List<BigInteger> result = new ArrayList<>();
    Collections.addAll(
        result,
        currentBlock.getSamplesByRadiusByColumn(
            peerId,
            currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));
    Collections.addAll(
        result,
        currentBlock.getSamplesByRadiusByRow(
            peerId,
            currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER)));

    return result.toArray(new BigInteger[0]);
  }

  /*public List<BigInteger> getReceivedSamples() {
    return samples;
  }*/

  public boolean completed() {
    // System.out.println("Samples num " + samples.size());
    if (samples.size() >= KademliaCommonConfigDas.N_SAMPLES) return true;
    else return false;
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
    return nextNodes.toArray(new BigInteger[0]);
  }*/

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
}
