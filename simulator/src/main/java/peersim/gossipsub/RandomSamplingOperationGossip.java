package peersim.gossipsub;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.MissingNode;
// import peersim.kademlia.das.SearchTable;
import peersim.kademlia.das.operations.Node;
import peersim.kademlia.das.operations.RandomSamplingOperation;

/**
 * This class represents a random sampling operation that collects samples from random nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */
public class RandomSamplingOperationGossip extends RandomSamplingOperation {

  // PeerTable peers;
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
      PeerTable searchTable,
      boolean isValidator,
      // int numValidators,
      MissingNode callback) {
    // super(srcNode, destNode, timestamp, currentBlock, isValidator, numValidators, callback);
    super(srcNode, destNode, timestamp, currentBlock, null, isValidator, callback);
    // this.peers = searchTable;
    System.out.println("New RandomSampling ");
  }

  /*protected void createNodes() {
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample).isDownloaded()) {

        List<BigInteger> validatorsBySampleRow =
            new ArrayList(peers.getPeers("Row" + samples.get(sample).getRow()));
        List<BigInteger> validatorsBySampleColumn =
            new ArrayList(peers.getPeers("Column" + samples.get(sample).getColumn()));

        boolean found = false;
        List<BigInteger> validatorsBySample = new ArrayList<>();

        validatorsBySample.addAll(validatorsBySampleRow);
        validatorsBySample.addAll(validatorsBySampleColumn);

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

        if (!found && callback != null) callback.missing(sample, this);
      }
    }
    for (BigInteger n : nodes.keySet()) System.out.println("Node crated " + n);
  }*/
  protected void createNodes() {}

  protected boolean createNodes(PeerTable peers, BigInteger builder) {
    for (BigInteger sample : samples.keySet()) {
      if (!samples.get(sample).isDownloaded()) {

        List<BigInteger> validatorsBySampleRow =
            new ArrayList(peers.getPeers("Row" + samples.get(sample).getRow()));
        List<BigInteger> validatorsBySampleColumn =
            new ArrayList(peers.getPeers("Column" + samples.get(sample).getColumn()));
        validatorsBySampleRow.remove(builder);
        validatorsBySampleColumn.remove(builder);

        /*System.out.println(
            "Node Row"
                + samples.get(sample).getRow()
                + " "
                + peers.getPeers("Row" + samples.get(sample).getRow()).size());

        System.out.println(
            "Node column"
                + samples.get(sample).getRow()
                + " "
                + peers.getPeers("Column" + samples.get(sample).getColumn()).size());*/
        boolean found = false;
        List<BigInteger> validatorsBySample = new ArrayList<>();

        validatorsBySample.addAll(validatorsBySampleRow);
        validatorsBySample.addAll(validatorsBySampleColumn);

        validatorsBySample.remove(builder);

        if (validatorsBySample != null && validatorsBySample.size() > 0) {
          for (BigInteger id : validatorsBySample) {
            if (!nodes.containsKey(id)) {
              nodes.put(id, new Node(id));
              nodes.get(id).addSample(samples.get(sample));
            } else {
              nodes.get(id).addSample(samples.get(sample));
            }
          }
          found = true;
        }
        if (!found) return found;
        if (!found && callback != null) callback.missing(sample, this);
      }
    }
    return true;
    // System.out.println("Builder " + builder);
    // for (BigInteger n : nodes.keySet()) System.out.println("Node crated " + n);
  }
}
