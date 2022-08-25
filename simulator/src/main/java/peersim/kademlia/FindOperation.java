package peersim.kademlia;

import java.math.BigInteger;
import java.util.HashMap;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the
 * closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class FindOperation {

  /** unique sequence number generator of the operation */
  private static long OPERATION_ID_GENERATOR = 0;

  /** represent univocally the find operation */
  public long operationId;

  /** Id of the node to find */
  public BigInteger destNode;

  /** Body of the original find message */
  public Object body;

  /** number of available find request message to send (it must be always less than ALPHA) */
  public int available_requests;

  /** Start timestamp of the search operation */
  protected long timestamp = 0;

  /** Number of hops the message did */
  protected int nrHops = 0;

  /** Boolean to indicate whether the operation is completed because the node is found */
  protected boolean finished;
  /**
   * This map contains the K closest nodes and corresponding boolean value that indicates if the
   * nodes has been already queried or not
   */
  protected HashMap<BigInteger, Boolean> closestSet;

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public FindOperation(BigInteger destNode, long timestamp) {
    this.destNode = destNode;
    this.timestamp = timestamp;

    // set a new find id
    operationId = OPERATION_ID_GENERATOR++;

    // set availabe request to ALPHA
    available_requests = KademliaCommonConfig.ALPHA;

    // initialize closestSet
    closestSet = new HashMap<BigInteger, Boolean>();
  }

  /**
   * update closestSet with the new information received
   *
   * @param neighbours
   */
  public void elaborateResponse(BigInteger[] neighbours) {
    // update responseNumber
    available_requests++;

    // add to closestSet
    for (BigInteger n : neighbours) {

      if (n != null) {
        if (!closestSet.containsKey(n)) {
          if (closestSet.size() < KademliaCommonConfig.K) { // add directly
            closestSet.put(n, false);
          } else { // find in the closest set if there are nodes whit less distance
            int newdist = Util.distance(n, destNode);

            // find the node with max distance
            int maxdist = newdist;
            BigInteger nodemaxdist = n;
            for (BigInteger i : closestSet.keySet()) {
              int dist = Util.distance(i, destNode);

              if (dist > maxdist) {
                maxdist = dist;
                nodemaxdist = i;
              }
            }

            if (nodemaxdist.compareTo(n) != 0) {
              closestSet.remove(nodemaxdist);
              closestSet.put(n, false);
            }
          }
        }
      }
    }

    /*String s = "closestSet to " + destNode + "\n";
    for (BigInteger clos : closestSet.keySet()) {
    	 s+= clos + "-";
    }
    System.out.println(s);*/

  }

  /**
   * get the first neighbour in closest set wich has not been already queried
   *
   * @return the Id of the node or null if there aren't available node
   */
  public BigInteger getNeighbour() {
    // find closest neighbour ( the first not already queried)
    BigInteger res = null;
    for (BigInteger n : closestSet.keySet()) {
      if (n != null && closestSet.get(n) == false) {
        if (res == null) {
          res = n;
        } else if (Util.distance(n, destNode) < Util.distance(res, destNode)) {
          res = n;
        }
      }
    }

    // Has been found a valid neighbour
    if (res != null) {
      closestSet.remove(res);
      closestSet.put(res, true);
      available_requests--; // decrease available request
    }

    return res;
  }

  public boolean isFinished() {
    return this.finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }
}
