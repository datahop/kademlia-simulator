package peersim.kademlia.das.operations;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.kademlia.das.SearchTable;
import peersim.kademlia.operations.FindOperation;

/**
 * This class represents a random sampling operation and offer the methods needed to maintain and
 * update the closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RandomSamplingOperation extends FindOperation {

  private SearchTable rou;

  /**
   * defaul constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public RandomSamplingOperation(
      BigInteger srcNode, BigInteger destNode, SearchTable rou, long timestamp) {
    super(srcNode, destNode, timestamp);
    this.rou = rou;

    for (BigInteger id : rou.getAllNeighbours()) closestSet.put(id, false);
  }

  public BigInteger getNeighbour() {

    BigInteger res = null;

    if (closestSet.size() > 0) {
      BigInteger[] results = (BigInteger[]) closestSet.keySet().toArray();
      res = results[CommonState.r.nextInt(results.length)];
    }

    if (res != null) {
      closestSet.remove(res);
      closestSet.put(res, true);
      // increaseUsed(res);
      this.available_requests--; // decrease available request
    }
    return res;
  }

  public boolean completed() {
    return false;
  }
}
